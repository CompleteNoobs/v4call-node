// ── v4call-node/escrow-box-client.mjs — node-side escrow-protocol/0.1 transport ──
//
// The node half of the escrow seam, MIRRORING v4call-escrow/nostr-transport.mjs. In box mode
// (ESCROW_MODE=box) the keyless node PUBLISHES signed `event-report`s addressed to the escrow
// box's reporting pubkey and SUBSCRIBES for `settlement-receipt`s addressed to itself, over the
// same nGate relays it already federates on. Symmetric with the box: same kind (31337), same
// tags. The trust gate is the INNER escrow-protocol schnorr signature (escrow-core
// signReport/verifyReport) — the Nostr event sig + the event-id seenIds are only delivery and
// relay-level dedup. A relay can't forge a receipt it can't inner-sign under the box key, and
// the node verifies every receipt under the pinned ESCROW_BOX_PUBKEY (see escrow-box-mode.js).
//
// ESM (nostr-tools v2 is ESM-only); loaded from server.js via dynamic import(), exactly like
// nostr-fed.mjs. Sets up the Node ws + webcrypto shims the same way (harmless if already set).

import { SimplePool, useWebSocketImplementation } from 'nostr-tools/pool';
import { finalizeEvent, verifyEvent } from 'nostr-tools/pure';
import WebSocket from 'ws';
import { webcrypto } from 'node:crypto';

if (!globalThis.crypto) globalThis.crypto = webcrypto;
useWebSocketImplementation(WebSocket);

const ESCROW_KIND = 31337;            // same dedicated kind as the box transport
const TAG_TOPIC = 'escrow-protocol';

function hexToBytes(hex) {
  const clean = String(hex || '').trim();
  const a = new Uint8Array(clean.length / 2);
  for (let i = 0; i < a.length; i++) a[i] = parseInt(clean.slice(i * 2, i * 2 + 2), 16);
  return a;
}

/**
 * Create the node-side escrow box client.
 *
 * @param relays      string[] of wss:// relay URLs (the nGate relays — default NOSTR_RELAYS)
 * @param selfSkHex   64-hex schnorr sk that signs the Nostr events this node publishes. This IS
 *                    the node's escrow-reporting key (== its NOSTR identity sk_hex); its pubkey
 *                    must be in the box's ESCROW_EXPECTED_REPORTERS.
 * @param boxPubkey   64-hex schnorr pubkey of the escrow box (reports are addressed ['p', box]).
 * @param now         () => epoch ms  (created_at source; injectable for tests)
 * @param log         (level, msg) => void
 *
 * Returns { start(onReceipt), publishReport(signedReport), close(), selfPub, ESCROW_KIND }:
 *   - start(onReceipt): subscribe for settlement-receipts tagged to selfPub; onReceipt(payload)
 *     per inbound event (escrow-box-mode does the real verifyReceiptFromBox).
 *   - publishReport(signedReport): wrap the signed event-report in a kind-31337 event tagged
 *     ['p', boxPubkey] and broadcast it to all relays.
 */
export function createEscrowBoxClient({ relays, selfSkHex, boxPubkey, now = () => Date.now(), log = () => {} }) {
  if (!Array.isArray(relays) || relays.length === 0) throw new Error('escrow-box-client: relays[] required');
  if (!/^[0-9a-f]{64}$/i.test(String(selfSkHex || ''))) throw new Error('escrow-box-client: selfSkHex must be 64-hex');
  if (!/^[0-9a-f]{64}$/i.test(String(boxPubkey || ''))) throw new Error('escrow-box-client: boxPubkey must be 64-hex');
  const skBytes = hexToBytes(selfSkHex);
  const selfPub = finalizeEvent({ kind: ESCROW_KIND, content: '', tags: [], created_at: 0 }, skBytes).pubkey;
  const pool = new SimplePool();
  const seenEventIds = new Set();
  let sub = null;

  function start(onReceipt) {
    // NB: subscribeMany takes a SINGLE filter object (it wraps it internally). Passing an
    // array double-wraps it → strict relays (strfry) reject "filter is not an object".
    sub = pool.subscribeMany(relays, { kinds: [ESCROW_KIND], '#p': [selfPub] }, {
      onevent: (ev) => {
        try {
          if (seenEventIds.has(ev.id)) return;            // relay-level one-shot
          seenEventIds.add(ev.id);
          if (seenEventIds.size > 5000) seenEventIds.clear();
          if (!verifyEvent(ev)) return;                   // malformed event — drop (inner sig is the real gate)
          const payload = JSON.parse(ev.content);
          Promise.resolve(onReceipt(payload)).catch(e => log('error', `onReceipt threw: ${e.message}`));
        } catch (e) { log('warn', `bad escrow event ${ev && ev.id}: ${e.message}`); }
      },
    });
    log('info', `subscribed for settlement-receipts as ${selfPub.slice(0, 12)}… on ${relays.length} relay(s)`);
  }

  async function publishReport(signedReport) {
    const ev = finalizeEvent(
      { kind: ESCROW_KIND, content: JSON.stringify(signedReport),
        tags: [['t', TAG_TOPIC], ['p', boxPubkey]], created_at: Math.floor(now() / 1000) },
      skBytes
    );
    const results = await Promise.allSettled(pool.publish(relays, ev));
    const ok = results.filter(r => r.status === 'fulfilled').length;
    log('info', `published ${signedReport.type} for ${signedReport.ref} → box ${boxPubkey.slice(0, 12)}… (${ok}/${relays.length} relay(s))`);
    return { id: ev.id, accepted: ok };
  }

  function close() { try { if (sub) sub.close(); } catch {} try { pool.close(relays); } catch {} }

  return { start, publishReport, close, selfPub, ESCROW_KIND };
}
