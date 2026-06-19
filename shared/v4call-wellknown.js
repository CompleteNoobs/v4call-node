// ── v4call shared — well-known domain-proof canonical payload ─────────────────
// The Hive posting key signs THIS exact string; a verifier (the node, nGate, or
// a peer) recomputes it byte-for-byte to check the signature. The signer lives
// in the browser (v4call-app/server-sign.html); the verifier lives in the node
// (v4call-node/server.js). If the two ever disagree on field order or the
// Nostr-append rule, every signature silently fails to verify — which is exactly
// the drift this shared module exists to prevent.
//
// VENDORED, NOT a package: an identical copy lives in BOTH repos
//   v4call-node/shared/v4call-wellknown.js   and   v4call-app/shared/v4call-wellknown.js
// Keep them byte-identical (handover §10 — the browser has no build step, so this
// is plain JS usable as a <script src> global AND a Node require()).
//
// Wire compatibility is fixed: this is the federation 0.4 domain-proof string —
// do NOT reorder fields or change the join. New fields are additive only, and
// only behind a presence check (as nostr_* already is) so old signed files keep
// verifying.
(function (root, factory) {
  if (typeof module === 'object' && module.exports) {
    module.exports = factory();                 // Node: require('./shared/v4call-wellknown')
  } else {
    root.V4callWellKnown = factory();           // Browser: window.V4callWellKnown
  }
})(typeof globalThis !== 'undefined' ? globalThis : (typeof self !== 'undefined' ? self : this), function () {
  'use strict';

  // Build the canonical '|'-joined string the Hive posting key signs.
  //
  // 9-field base (always): claim, domain, hive_account, escrow, fee_account,
  // federation_ws, issued, expires (|| ''), nonce.
  // +3 Nostr fields (npub, hex, relays.join(',')) appended ONLY when any Nostr
  // field is present, so a pre-Nostr file produces the original 9-field shape
  // and keeps verifying. nostr_attestation is INTENTIONALLY excluded (it is
  // self-verifying via its own schnorr sig + tag cross-check).
  function buildDomainProofPayload(obj) {
    const base = [
      obj.claim,
      obj.domain,
      obj.hive_account,
      obj.escrow,
      obj.fee_account,
      obj.federation_ws,
      obj.issued,
      obj.expires || '',
      obj.nonce
    ];
    const relays   = Array.isArray(obj.nostr_relays) ? obj.nostr_relays.filter(Boolean) : [];
    const hasNostr = !!(obj.nostr_npub || obj.nostr_hex || relays.length);
    if (hasNostr) {
      base.push(
        obj.nostr_npub || '',
        obj.nostr_hex  || '',
        relays.join(',')
      );
    }
    return base.join('|');
  }

  return { buildDomainProofPayload: buildDomainProofPayload };
});
