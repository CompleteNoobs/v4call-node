// escrow-report.js — the escrow-protocol/0.1 report/receipt SEAM (handover §6).
//
// Expresses settlement as a node→escrow `event-report` (the metering facts) and an
// escrow→node `settlement-receipt` (the outcome), each signed with the node's NOSTR
// reporting key (schnorr/BIP340 — the SAME key family as nostr-fed.mjs; escrow-core's
// sigs are interoperable). In-process today; the Step-5 box extraction is then just
// "move the escrow handler + key to another host and swap the in-process call for a
// Nostr round-trip" — identical payloads, no logic change.
//
// SAFETY (in-process): the reporter signs AND verifies with the SAME key, so a verify
// failure means a bug, not an attack. Every method here is defensive (never throws) and
// the caller treats the seam as best-effort — settlement is gated by the durable ledger
// (atomicClose + tx_id UNIQUE), NEVER by this seam. On the isolated escrow box the
// transport is untrusted, so THERE verifyReport/markSeen become hard gates.
//
// Sig family note: escrow-protocol (Nostr schnorr) is DISTINCT from any Hive
// release-consent sig — never merge the two.

'use strict';

const crypto = require('crypto');

function createEscrowReporter({ escrowCore, getSkHex, reporter, service = 'v4call', maxSeen = 5000 }) {
  if (!escrowCore || typeof getSkHex !== 'function') {
    throw new Error('createEscrowReporter: escrowCore and getSkHex are required');
  }
  const reporterId = typeof reporter === 'function' ? reporter : () => reporter;
  const seen = escrowCore.createSeenIds(maxSeen);

  // Cache the derived pubkey per secret (the key is stable once nostr-fed writes it).
  let _pub = null, _pubForSk = null;
  function pubkey() {
    const sk = safeSk();
    if (!sk) return null;
    if (sk !== _pubForSk) {
      try { _pub = escrowCore.getReportingPubkey(sk); _pubForSk = sk; }
      catch { _pub = null; _pubForSk = null; }
    }
    return _pub;
  }
  function safeSk() {
    try { const sk = getSkHex(); return (typeof sk === 'string' && /^[0-9a-f]{64}$/i.test(sk)) ? sk.toLowerCase() : null; }
    catch { return null; }
  }

  /** A stable one-shot nonce for a settlement event (redelivery of the SAME event dedups). */
  function settleNonce(ref) { return `${ref}:settle`; }
  /** A random nonce (when no natural one-shot key exists). */
  function randomNonce() { return crypto.randomBytes(16).toString('hex'); }

  // ── NODE → ESCROW: build + sign an event report. Returns the signed payload, or
  // null if the reporting key isn't available yet (seam degraded → caller proceeds).
  function buildSignedReport({ ref, subject, facts, nonce, createdAt }) {
    const sk = safeSk();
    if (!sk) return null;
    try {
      const report = escrowCore.buildEventReport({ service, ref, subject, facts, nonce, createdAt, reporter: reporterId() });
      return escrowCore.signReport(report, sk);
    } catch (e) {
      console.warn(`[escrow-report] could not build/sign event report for ${ref}: ${e.message}`);
      return null;
    }
  }

  // ── ESCROW side: verify the report sig (against our reporting pubkey) + mark the
  // nonce seen. Returns a verdict; NON-throwing, NON-gating in-process (the caller
  // logs it). `fresh:false` = a duplicate report (the box would drop it here).
  function acceptReport(signed) {
    if (!signed) return { unsigned: true, verified: false, fresh: true, replay: false };
    let verified = false;
    try { verified = escrowCore.verifyReport(signed, pubkey() || undefined); } catch { verified = false; }
    const fresh = signed.nonce ? seen.markSeen(signed.nonce) : true;
    return { unsigned: false, verified, fresh, replay: !fresh };
  }

  // ── ESCROW → NODE: build + sign a settlement receipt (signed if the key is available,
  // otherwise the bare canonical payload).
  function buildSignedReceipt({ ref, settlement, refund, dust, currency, disburseTx, status, createdAt }) {
    try {
      const receipt = escrowCore.buildSettlementReceipt({ ref, settlement, refund, dust, currency, disburseTx, status, createdAt });
      const sk = safeSk();
      return sk ? escrowCore.signReport(receipt, sk) : receipt;
    } catch (e) {
      console.warn(`[escrow-report] could not build/sign settlement receipt for ${ref}: ${e.message}`);
      return null;
    }
  }

  // ── NODE side: verify a settlement receipt's sig.
  function verifyReceipt(signed) {
    try { return !!(signed && signed.sig && escrowCore.verifyReport(signed, pubkey() || undefined)); }
    catch { return false; }
  }

  // ── NODE side (BOX MODE): verify a settlement receipt signed by the BOX. In-process the node
  // signs AND verifies the receipt with its own key (verifyReceipt above); in box mode the box
  // signs it with ITS reporting key, so the node must verify under the pinned box pubkey
  // (ESCROW_BOX_PUBKEY) — not its own. Fails closed on a bad/absent sig or a non-hex pubkey.
  function verifyReceiptFromBox(signed, boxPubkey) {
    try {
      return !!(signed && signed.sig && /^[0-9a-f]{64}$/i.test(String(boxPubkey || ''))
        && escrowCore.verifyReport(signed, String(boxPubkey).toLowerCase()));
    } catch { return false; }
  }

  return { buildSignedReport, acceptReport, buildSignedReceipt, verifyReceipt, verifyReceiptFromBox, pubkey, settleNonce, randomNonce };
}

module.exports = { createEscrowReporter };
