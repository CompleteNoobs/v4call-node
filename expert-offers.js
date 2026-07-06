// ── v4call-node/expert-offers.js — the v0.17 paid-expert-offer state machine ──
//
// Pure, injectable-clock lifecycle tracking for paid expert offers (v0.17 Part A,
// local/same-server). NO money moves here — server.js drives escrow verify/refund/
// settle around these transitions; this module only guards WHO may transition WHAT
// and WHEN, so the rules are unit-testable outside the server.js monolith (same
// seam pattern as escrow-verify.js / escrow-box-mode.js).
//
// The design contract (CLAUDE.md "Planned Features → v0.17", locked in):
//   · the INVITE IS THE CONTRACT — admin sets connectFee + ratePerHour + currency +
//     maxDurationMin; accept = consent to those exact terms (expert's rate post ignored)
//   · inviter holds the funds — the cap (connectFee + maxDuration×rate) is paid up
//     front into THIS server's escrow; the expert only ever receives what settlement
//     disburses (rug-pull protection: INVERTS the 1:1 paid-call treasurer)
//   · each offer is its own contract — re-invite = a fresh offer, fresh terms
//   · ONE non-terminal offer at a time per inviter (v0.17 first-build simplification)
//
// Lifecycle:
//   awaiting_payment ──fund──▶ offered ──accept──▶ accepted ──joined──▶ active
//        │TTL                  │decline/TTL         │TTL(join)           │exit
//        ▼                     ▼                    ▼                    ▼
//     expired               refunding            refunding            settling
//    (no funds moved)          └─▶ refunded         └─▶ refunded         └─▶ settled
//
// Terminal states: expired · refunded · settled · refund_failed (parked for operator).
// Every transition is single-winner: it checks the exact from-state, so a concurrent
// decline-vs-settle (or a doubled sweep) can never move the same money twice — and in
// box mode the settlement queue's UNIQUE(ref) backstops this durably.

'use strict';

const crypto = require('crypto');

const NON_TERMINAL = new Set(['awaiting_payment', 'offered', 'accepted', 'active', 'settling', 'refunding']);

function createExpertOffers({
  now = Date.now,
  offerTtlMs = 15 * 60 * 1000,   // awaiting_payment / offered lifetime (mirrors pendingPaidInvites)
  joinTtlMs  = 5 * 60 * 1000,    // accepted → must actually join within this window
  maxDurationCapMin = 720,       // hard ceiling on maxDurationMin an admin may set
  log = () => {},
} = {}) {
  const offers = new Map();      // offerId → offer

  const round = (n, places) => parseFloat(Number(n).toFixed(places));

  /**
   * Create an offer in 'awaiting_payment'. Validates the terms and the one-offer-per-
   * inviter rule. Returns { ok:true, offer } or { ok:false, reason } (human-readable —
   * server.js surfaces reason to the admin verbatim).
   */
  function create({ room, inviter, expert, connectFee, ratePerHour, currency, maxDurationMin, precision = 3 }) {
    if (!room || typeof room !== 'string')       return { ok: false, reason: 'Missing room.' };
    if (!inviter || typeof inviter !== 'string') return { ok: false, reason: 'Missing inviter.' };
    if (!expert || typeof expert !== 'string')   return { ok: false, reason: 'Missing expert.' };
    if (expert === inviter)                      return { ok: false, reason: 'You cannot invite yourself as a paid expert.' };
    if (!currency || typeof currency !== 'string' || !/^[A-Z0-9.]{1,12}$/.test(currency)) {
      return { ok: false, reason: 'Invalid currency.' };
    }
    const fee  = Number(connectFee);
    const rate = Number(ratePerHour);
    const mins = Number(maxDurationMin);
    if (!Number.isFinite(fee)  || fee  < 0) return { ok: false, reason: 'Connect fee must be a number ≥ 0.' };
    if (!Number.isFinite(rate) || rate < 0) return { ok: false, reason: 'Rate must be a number ≥ 0.' };
    if (!Number.isInteger(mins) || mins < 1 || mins > maxDurationCapMin) {
      return { ok: false, reason: `Max duration must be a whole number of minutes between 1 and ${maxDurationCapMin}.` };
    }
    const places = Number.isInteger(precision) && precision >= 0 && precision <= 8 ? precision : 3;
    const floor  = Math.pow(10, -places);
    const cap    = round(fee + rate * (mins / 60), places);
    if (cap < floor) {
      return { ok: false, reason: `An all-free offer has nothing to escrow — use a normal room invite instead (cap below ${floor} ${currency}).` };
    }
    const existing = activeByInviter(inviter);
    if (existing) {
      return { ok: false, reason: `You already have an offer in flight (to @${existing.expert}, #${existing.room}, ${existing.status}). One paid offer at a time — resolve it first.` };
    }

    const offerId = 'xo_' + crypto.randomBytes(9).toString('hex');   // 'xo' = expert offer; distinct ref namespace
    const offer = {
      offerId, room, inviter, expert,
      connectFee: round(fee, places), ratePerHour: round(rate, places),
      currency, maxDurationMin: mins, precision: places, cap,
      // The canonical refundable-cap purpose: the box's over-assert guard + per-call
      // column persistence recognize exactly {call, deposit, topup} as deposit rows,
      // so the connectFee carve (callFacts.connectPaid) only works with one of those.
      // The offerId in the ref keeps the namespace unambiguous.
      memo: `v4call:deposit:${offerId}`,
      status: 'awaiting_payment',
      createdAt: now(), txId: null, startTs: null, endedAt: null,
    };
    offers.set(offerId, offer);
    log('info', `offer ${offerId}: @${inviter} → @${expert} #${room} cap ${cap} ${currency} (awaiting payment)`);
    return { ok: true, offer };
  }

  function get(offerId) { return offers.get(offerId) || null; }

  /** The inviter's single in-flight offer, if any (the one-at-a-time gate). */
  function activeByInviter(inviter) {
    for (const o of offers.values()) if (o.inviter === inviter && NON_TERMINAL.has(o.status)) return o;
    return null;
  }

  /** The expert's offer for a room in one of the given states (settlement/join lookups). */
  function findByExpert(expert, room, statuses) {
    const want = new Set(statuses);
    for (const o of offers.values()) if (o.expert === expert && o.room === room && want.has(o.status)) return o;
    return null;
  }

  /** Guarded transition helper: from exact state → to state, with an ownership check. */
  function _move(offerId, who, whoField, from, to) {
    const o = offers.get(offerId);
    if (!o) return { ok: false, reason: 'Unknown offer.' };
    if (whoField && o[whoField] !== who) return { ok: false, reason: 'Not your offer.' };
    if (o.status !== from) return { ok: false, reason: `Offer is ${o.status}, not ${from}.` };
    o.status = to;
    return { ok: true, offer: o };
  }

  /** Payment verified on-chain → the offer may be delivered to the expert. */
  function fund(offerId, inviter, txId) {
    const r = _move(offerId, inviter, 'inviter', 'awaiting_payment', 'offered');
    if (r.ok) { r.offer.txId = txId; log('info', `offer ${offerId}: funded (tx ${txId})`); }
    return r;
  }

  /** Expert answers. accept → 'accepted' (join window opens). decline → 'refunding'. */
  function respond(offerId, expert, response) {
    if (response !== 'accept' && response !== 'decline') return { ok: false, reason: 'Invalid response.' };
    const to = response === 'accept' ? 'accepted' : 'refunding';
    const r = _move(offerId, expert, 'expert', 'offered', to);
    if (r.ok) log('info', `offer ${offerId}: ${response} by @${expert}`);
    return r;
  }

  /** Expert actually joined the room — the paid session starts metering NOW. */
  function joined(offerId, expert, startTs) {
    const r = _move(offerId, expert, 'expert', 'accepted', 'active');
    if (r.ok) { r.offer.startTs = startTs; log('info', `offer ${offerId}: session ACTIVE (start ${startTs})`); }
    return r;
  }

  /**
   * Claim settlement (single winner). Only an ACTIVE session settles for elapsed time;
   * the caller then runs the real settlement and marks the outcome. Returns the offer
   * or null if someone else already claimed it (double leave/disconnect race).
   */
  function beginSettle(offerId, endReason) {
    const o = offers.get(offerId);
    if (!o || o.status !== 'active') return null;
    o.status = 'settling';
    o.endedAt = now();
    o.endReason = endReason;
    log('info', `offer ${offerId}: settling (${endReason})`);
    return o;
  }

  /**
   * Claim a full-cap refund (single winner) from a pre-session state. Used by decline
   * (already 'refunding' via respond), sweeps, and room-death. Returns the offer or null.
   */
  function beginRefund(offerId, reason) {
    const o = offers.get(offerId);
    if (!o || !(o.status === 'offered' || o.status === 'accepted')) return null;
    o.status = 'refunding';
    o.endReason = reason;
    return o;
  }

  /** Terminal bookkeeping (server calls after the money actually moved / failed). */
  function markRefunded(offerId)     { const o = offers.get(offerId); if (o && o.status === 'refunding') o.status = 'refunded'; }
  function markRefundFailed(offerId) { const o = offers.get(offerId); if (o && o.status === 'refunding') o.status = 'refund_failed'; }
  function markSettled(offerId)      { const o = offers.get(offerId); if (o && o.status === 'settling')  o.status = 'settled'; }

  /**
   * TTL sweep. Applies transitions and returns the actions the server must execute:
   *   { offer, action:'expired' }  — awaiting_payment aged out; NO funds were verified, nothing to move
   *   { offer, action:'refund' }   — offered/accepted aged out; funds ARE in escrow → full refund
   * (An 'active' session never expires by TTL — settle() caps the money at the funded cap.)
   */
  function sweep(atMs = now()) {
    const actions = [];
    for (const o of offers.values()) {
      if (o.status === 'awaiting_payment' && atMs - o.createdAt > offerTtlMs) {
        o.status = 'expired';
        actions.push({ offer: o, action: 'expired' });
      } else if (o.status === 'offered' && atMs - o.createdAt > offerTtlMs) {
        o.status = 'refunding'; o.endReason = 'timed_out';
        actions.push({ offer: o, action: 'refund' });
      } else if (o.status === 'accepted' && atMs - o.createdAt > offerTtlMs + joinTtlMs) {
        o.status = 'refunding'; o.endReason = 'never_joined';
        actions.push({ offer: o, action: 'refund' });
      }
    }
    return actions;
  }

  /** Room died before/without a session — refund anything pre-active for that room.
   *  Returns the offers now in 'refunding' (server executes the refunds). Active
   *  sessions are NOT touched here: the member-exit path settles them for elapsed time. */
  function cancelForRoom(room, reason) {
    const out = [];
    for (const o of offers.values()) {
      if (o.room !== room) continue;
      if (o.status === 'awaiting_payment') { o.status = 'expired'; continue; }
      if (o.status === 'offered' || o.status === 'accepted') {
        o.status = 'refunding'; o.endReason = reason;
        out.push(o);
      }
    }
    return out;
  }

  return { create, get, activeByInviter, findByExpert, fund, respond, joined,
           beginSettle, beginRefund, markRefunded, markRefundFailed, markSettled,
           sweep, cancelForRoom, _offers: offers };
}

module.exports = { createExpertOffers };
