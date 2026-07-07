// ── v4call-node/escrow-box-mode.js — box-mode settlement orchestration (ESCROW_MODE=box) ──
//
// Ties together the three node-side pieces of the escrow split: the durable pending-report
// QUEUE (escrow-settlement-queue.js), the Nostr box CLIENT (escrow-box-client.mjs), and a
// DRAINER. In box mode the node is KEYLESS: it builds a call-end `event-report` from the call's
// durable shadow rows, hands it to the box, and finalizes (client receipt + legacy ledger + the
// federated receipt-back) only when the box returns a signed `settlement-receipt`. The BOX is
// the settlement authority — it re-verifies every payment on-chain and disburses with the only
// money key (handover-escrow-core §4/§9.1; "no Claude on the money box"). The node only reports
// and displays; it holds NO money key.
//
// IDEMPOTENCY: the queue's UNIQUE(ref) is the single-winner guard (replaces the in-process
// atomicClose); the stable `ref:settle` nonce lets the box dedup republished reports; and
// markSettled(pending→settled) fires the finalize handler EXACTLY once per call — whether the
// receipt arrives promptly or after a restart via the drainer.
//
// ROBUSTNESS: the queue stores the UNSIGNED facts; the drainer signs fresh on each publish, so a
// report enqueued before the reporting key is readable still settles once the key appears, and a
// transient publish failure simply retries on the next drain tick — "retry until received".

'use strict';

function createEscrowBoxMode({
  escrowAdapter, escrowReporter, queue, boxPubkey, relays, selfSkHex,
  maxDurationMin, drainIntervalMs = 30000, log = console,
  clientFactory = null,   // test seam: inject a fake transport; default = the real Nostr client
}) {
  if (!escrowAdapter || !escrowReporter || !queue) throw new Error('createEscrowBoxMode: escrowAdapter, escrowReporter, queue are required');
  if (!/^[0-9a-f]{64}$/i.test(String(boxPubkey || ''))) throw new Error('createEscrowBoxMode: boxPubkey must be 64-hex');

  const box = String(boxPubkey).toLowerCase();
  let client = null;
  let onSettledHandler = async () => {};
  let onCompletedHandler = async () => {};

  const L = {
    info: (m) => log.log && log.log(`[escrow-box] ${m}`),
    warn: (m) => log.warn && log.warn(`[escrow-box] ${m}`),
    error: (m) => log.error && log.error(`[escrow-box] ${m}`),
  };

  /** Register the finalize handler. Called EXACTLY once per settled call with
   *  (ref, { facts, receipt, meta }) — server.js emits the client receipt, updates the legacy
   *  ledger, and (federated) sends the caller's receipt back to their server. */
  function onSettled(fn) { onSettledHandler = fn; }

  /** Register the COMPLETION handler: a finalized-as-pending settlement whose payouts
   *  later landed via the box's recovery retry (or flipped to failed). Called EXACTLY
   *  once per pending→settled/failed transition with (ref, { facts, receipt, meta }) —
   *  server.js notifies both parties + upgrades the legacy ledger rows. */
  function onCompleted(fn) { onCompletedHandler = fn; }

  // Lazily create the transport. Returns the client, or null if the reporting key isn't readable
  // yet (the node's NOSTR identity, written by nostr-fed at boot) — settlements stay durably
  // queued and the drainer retries once the key appears.
  async function ensureClient() {
    if (client) return client;
    const sk = selfSkHex();
    if (!sk) return null;
    if (clientFactory) {
      client = clientFactory({ relays, selfSkHex: sk, boxPubkey: box, log: (lvl, m) => L.info(m) });
    } else {
      const { createEscrowBoxClient } = await import('./escrow-box-client.mjs');
      client = createEscrowBoxClient({ relays, selfSkHex: sk, boxPubkey: box, log: (lvl, m) => L.info(m) });
    }
    client.start(handleReceipt);
    L.info(`transport up — node reporting pubkey ${escrowReporter.pubkey() || '(deriving)'} (add it to the box's ESCROW_EXPECTED_REPORTERS)`);
    return client;
  }

  // Inbound box → node: a signed settlement-receipt. Verify under the BOX key, match a pending
  // report, finalize exactly once. Anything that doesn't verify under the pinned box key is dropped.
  async function handleReceipt(receipt) {
    const ref = receipt && receipt.ref;
    if (!ref) return;
    if (!escrowReporter.verifyReceiptFromBox(receipt, box)) {
      L.warn(`receipt for ${ref} failed verify under box key — ignored`);
      return;
    }
    const row = queue.get(ref);
    if (!row) { L.warn(`receipt for unknown/!queued ref ${ref} — ignored`); return; }
    // A status:'failed' receipt is the box's TERMINAL rejection of this report (e.g. the payment
    // landed in an account the box doesn't hold). Park the row as 'failed' — ends the drainer's
    // retry-until-received loop — and still fire the finalize handler exactly once so server.js
    // marks the legacy ledger rows failed (its dispatcher already handles status:'failed').
    const marked = receipt.status === 'failed'
      ? queue.markFailed(ref, receipt)
      : queue.markSettled(ref, receipt);
    if (marked) {
      let facts = {}, meta = null;
      try { facts = JSON.parse(row.facts_json); } catch {}
      try { meta = row.meta_json ? JSON.parse(row.meta_json) : null; } catch {}
      try { await onSettledHandler(ref, { facts, receipt, meta }); }
      catch (e) { L.error(`finalize ${ref} threw: ${e.message}`); }
      if (receipt.status === 'failed') {
        L.error(`${ref} REJECTED by box (terminal): ${receipt.reason || 'no reason given'} — parked as failed; nothing was disbursed. Operator action needed if a real payment is stranded.`);
      } else {
        L.info(`settled ${ref} via box: settlement=${receipt.settlement} refund=${receipt.refund} status=${receipt.status}`);
      }
    } else {
      // Row already terminal. If the STORED receipt said 'pending' and this one is a
      // definitive settled/failed, it's the box's COMPLETION receipt — the recovery
      // retry finished the payouts (or gave up). Fire onCompleted EXACTLY once for the
      // pending→terminal transition; duplicates (stored no longer pending) are no-ops.
      let stored = null;
      try { stored = row.receipt_json ? JSON.parse(row.receipt_json) : null; } catch {}
      if (stored && stored.status === 'pending' && (receipt.status === 'settled' || receipt.status === 'failed')) {
        queue.updateReceipt(ref, receipt);
        let facts = {}, meta = null;
        try { facts = JSON.parse(row.facts_json); } catch {}
        try { meta = row.meta_json ? JSON.parse(row.meta_json) : null; } catch {}
        try { await onCompletedHandler(ref, { facts, receipt, meta }); }
        catch (e) { L.error(`onCompleted ${ref} threw: ${e.message}`); }
        L.info(`${ref} settlement COMPLETED (was pending): status=${receipt.status}`);
      } // else: plain duplicate receipt — no-op
    }
  }

  // Sign (fresh, under the stable nonce) and publish one pending row. Stays pending on any failure
  // (no key yet / transient relay error) so the next drain retries — never lost.
  async function publishRow(row) {
    const c = await ensureClient();
    if (!c) { L.warn(`${row.ref}: reporting key not ready — stays queued`); return false; }
    let facts; try { facts = JSON.parse(row.facts_json); } catch (e) { L.error(`${row.ref}: corrupt facts_json — skipping`); return false; }
    const signed = escrowReporter.buildSignedReport({ ref: row.ref, subject: row.ref, facts, nonce: row.nonce, createdAt: row.created_at });
    if (!signed) { L.warn(`${row.ref}: could not sign report (key not ready) — stays queued`); return false; }
    queue.markAttempt(row.ref, Date.now());
    try { await c.publishReport(signed); return true; }
    catch (e) { L.warn(`${row.ref}: publish failed (will retry): ${e.message}`); return false; }
  }

  // Republish every still-pending report (boot recovery + the periodic retry).
  async function drainOnce() {
    const rows = queue.pending();
    for (const row of rows) await publishRow(row);
    return rows.length;
  }

  /**
   * Settle a call via the box. Builds the call-end envelope from the call's durable shadow rows,
   * durably enqueues it (single-winner), and publishes. Finalization happens asynchronously in
   * handleReceipt when the box returns a signed receipt. NON-blocking on the box's response.
   *
   * @param callId         the call/reservation id (the report `ref`)
   * @param payRows        escrowLedger.getPaymentsByRef(callId)
   * @param endReason      why the call ended
   * @param now            settlement-clock epoch ms
   * @param meta           optional finalize context (e.g. { callerServer, federated } for fed calls)
   * @returns true iff a NEW report was enqueued (false on a duplicate call-end)
   */
  async function settleCall({ callId, payRows, endReason, now, meta = null }) {
    const facts = escrowAdapter.buildCallEndReportFacts({ payRows, endReason, now, maxDurationMin });
    const nonce = escrowReporter.settleNonce(callId);
    const won = queue.enqueue(callId, facts, nonce, now, meta);
    if (!won) { L.info(`${callId} already queued (duplicate call-end) — ignored`); return false; }
    await publishRow(queue.get(callId));
    return true;
  }

  /**
   * Settle a single (non-call) payment via the box — paid DMs, attachments, invites, and
   * ring-fee refunds. Same durable enqueue + publish + async-finalize-on-receipt pattern as
   * settleCall, but for the report/split shape built by buildSinglePaymentReportFacts (no
   * duration/cap concept — the whole verified amount splits into net + fee, or, platformFee 0,
   * is a pure refund back to payoutTo).
   *
   * @param ref            unique settlement ref (msgId / inviteId / callId — must not collide
   *                       with any other settleCall/settlePayment ref on this node)
   * @param txId/sender/amount/currency/memo   the ONE on-chain deposit the box will re-verify
   * @param payoutTo       recipient of the net payout (the original sender, for a refund)
   * @param platformFee    0..1 fraction to the box's configured feeAccount; 0 = pure refund
   * @param now            settlement-clock epoch ms
   * @param meta           finalize context for onSettled (server.js dispatches on meta.kind)
   * @returns true iff a NEW report was enqueued (false on a duplicate)
   */
  async function settlePayment({ ref, txId, sender, amount, currency, memo, payoutTo, platformFee = 0, now, meta = null }) {
    const facts = escrowAdapter.buildSinglePaymentReportFacts({ txId, sender, amount, currency, memo, payoutTo, platformFee });
    const nonce = escrowReporter.settleNonce(ref);
    const won = queue.enqueue(ref, facts, nonce, now, meta);
    if (!won) { L.info(`${ref} already queued (duplicate settle) — ignored`); return false; }
    await publishRow(queue.get(ref));
    return true;
  }

  // Boot: bring up the transport (if the key is ready) and drain any reports left pending from a
  // prior run, then retry on an interval. Never throws (a transport problem must not crash v4call).
  async function start() {
    try { await ensureClient(); } catch (e) { L.error(`transport init failed (will retry on drain): ${e.message}`); }
    try { const n = await drainOnce(); if (n) L.info(`drained ${n} pending report(s) on boot`); } catch (e) { L.error(`boot drain failed: ${e.message}`); }
    const t = setInterval(() => { drainOnce().catch(e => L.error(`drain tick failed: ${e.message}`)); }, drainIntervalMs);
    if (t && typeof t.unref === 'function') t.unref();
  }

  return { start, settleCall, settlePayment, onSettled, onCompleted, drainOnce, _handleReceipt: handleReceipt };
}

module.exports = { createEscrowBoxMode };
