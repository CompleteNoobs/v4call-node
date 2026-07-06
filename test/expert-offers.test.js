// ── test/expert-offers.test.js — the v0.17 paid-expert-offer state machine ──
//
// Pure lifecycle rules, no money: server.js drives escrow verify/refund/settle AROUND
// these transitions, so what's proven here is that the transitions themselves can never
// authorize the same funds twice, never let the wrong party move an offer, and enforce
// the locked-in v0.17 rules (invite-is-the-contract terms validation, one offer per
// inviter, single-winner settle vs refund).

'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { createExpertOffers } = require('../expert-offers');

const T0 = 1_700_000_000_000;

function mk(overrides = {}) {
  let t = T0;
  const clock = { now: () => t, tick: (ms) => { t += ms; } };
  const xo = createExpertOffers({ now: clock.now, ...overrides });
  return { xo, clock };
}

const TERMS = { room: 'warroom', inviter: 'cnoobz', expert: 'guest33',
  connectFee: 0.5, ratePerHour: 6, currency: 'TEST', maxDurationMin: 30, precision: 3 };

test('create: valid terms → awaiting_payment with the right cap (connect + maxDuration×rate)', () => {
  const { xo } = mk();
  const r = xo.create(TERMS);
  assert.equal(r.ok, true);
  assert.equal(r.offer.status, 'awaiting_payment');
  assert.equal(r.offer.cap, 3.5, '0.5 connect + 6/hr × 0.5h = 3.5');
  assert.equal(r.offer.memo, `v4call:deposit:${r.offer.offerId}`, 'deposit purpose — the box carves connect out of deposit rows only');
});

test('create: rejects self-invite, bad numbers, silly durations, all-free offers', () => {
  const { xo } = mk();
  assert.equal(xo.create({ ...TERMS, expert: 'cnoobz' }).ok, false, 'self');
  assert.equal(xo.create({ ...TERMS, connectFee: -1 }).ok, false, 'negative fee');
  assert.equal(xo.create({ ...TERMS, ratePerHour: NaN }).ok, false, 'NaN rate');
  assert.equal(xo.create({ ...TERMS, maxDurationMin: 0 }).ok, false, 'zero minutes');
  assert.equal(xo.create({ ...TERMS, maxDurationMin: 2.5 }).ok, false, 'fractional minutes');
  assert.equal(xo.create({ ...TERMS, maxDurationMin: 100000 }).ok, false, 'over cap');
  assert.equal(xo.create({ ...TERMS, currency: 'te$t' }).ok, false, 'bad currency');
  const free = xo.create({ ...TERMS, connectFee: 0, ratePerHour: 0 });
  assert.equal(free.ok, false, 'all-free offer has nothing to escrow');
});

test('create: ONE non-terminal offer per inviter (the v0.17 first-build rule)', () => {
  const { xo } = mk();
  assert.equal(xo.create(TERMS).ok, true);
  const second = xo.create({ ...TERMS, expert: 'noblemage' });
  assert.equal(second.ok, false);
  assert.match(second.reason, /One paid offer at a time/);
  // a DIFFERENT inviter is fine
  assert.equal(xo.create({ ...TERMS, inviter: 'guest33', expert: 'noblemage' }).ok, true);
});

test('happy path: fund → respond(accept) → joined → beginSettle wins exactly once', () => {
  const { xo } = mk();
  const { offer } = xo.create(TERMS);
  assert.equal(xo.fund(offer.offerId, 'cnoobz', 'tx1').ok, true);
  assert.equal(offer.status, 'offered');
  assert.equal(xo.respond(offer.offerId, 'guest33', 'accept').ok, true);
  assert.equal(offer.status, 'accepted');
  assert.equal(xo.joined(offer.offerId, 'guest33', T0 + 1000).ok, true);
  assert.equal(offer.status, 'active');
  assert.equal(offer.startTs, T0 + 1000);

  const claim1 = xo.beginSettle(offer.offerId, 'left_room');
  const claim2 = xo.beginSettle(offer.offerId, 'disconnect');   // the race loser
  assert.ok(claim1, 'first exit trigger claims settlement');
  assert.equal(claim2, null, 'second trigger gets nothing — no double settle');
  xo.markSettled(offer.offerId);
  assert.equal(offer.status, 'settled');
  // inviter is free to make a new offer now
  assert.equal(xo.create({ ...TERMS, expert: 'noblemage' }).ok, true);
});

test('ownership: only the inviter funds; only the expert responds/joins', () => {
  const { xo } = mk();
  const { offer } = xo.create(TERMS);
  assert.equal(xo.fund(offer.offerId, 'guest33', 'tx1').ok, false, 'expert cannot fund');
  xo.fund(offer.offerId, 'cnoobz', 'tx1');
  assert.equal(xo.respond(offer.offerId, 'cnoobz', 'accept').ok, false, 'inviter cannot accept own offer');
  xo.respond(offer.offerId, 'guest33', 'accept');
  assert.equal(xo.joined(offer.offerId, 'someoneelse', T0).ok, false, 'stranger cannot join the contract');
});

test('decline: offered → refunding (single-winner vs a concurrent settle path)', () => {
  const { xo } = mk();
  const { offer } = xo.create(TERMS);
  xo.fund(offer.offerId, 'cnoobz', 'tx1');
  assert.equal(xo.respond(offer.offerId, 'guest33', 'decline').ok, true);
  assert.equal(offer.status, 'refunding');
  assert.equal(xo.beginSettle(offer.offerId, 'left_room'), null, 'declined offer can never settle');
  assert.equal(xo.respond(offer.offerId, 'guest33', 'accept').ok, false, 'decline is final');
  xo.markRefunded(offer.offerId);
  assert.equal(offer.status, 'refunded');
});

test('out-of-order and premature transitions are refused', () => {
  const { xo } = mk();
  const { offer } = xo.create(TERMS);
  assert.equal(xo.respond(offer.offerId, 'guest33', 'accept').ok, false, 'cannot accept before funding');
  assert.equal(xo.joined(offer.offerId, 'guest33', T0).ok, false, 'cannot join before accept');
  assert.equal(xo.beginSettle(offer.offerId, 'x'), null, 'cannot settle before active');
  assert.equal(xo.get('xo_nope'), null);
  assert.equal(xo.fund('xo_nope', 'cnoobz', 'tx').ok, false);
});

test('sweep: awaiting_payment expires with NO refund action; offered refunds; accepted-but-never-joined refunds', () => {
  const { xo, clock } = mk();
  const a = xo.create({ ...TERMS, inviter: 'a1' }).offer;                       // stays awaiting_payment
  const b = xo.create({ ...TERMS, inviter: 'b1' }).offer; xo.fund(b.offerId, 'b1', 'txb');   // offered
  const c = xo.create({ ...TERMS, inviter: 'c1' }).offer; xo.fund(c.offerId, 'c1', 'txc');
  xo.respond(c.offerId, 'guest33', 'accept');                                   // accepted, never joins

  clock.tick(16 * 60 * 1000);            // past offerTtl (15m), inside join grace for c
  let actions = xo.sweep();
  const byRef = Object.fromEntries(actions.map(x => [x.offer.offerId, x.action]));
  assert.equal(byRef[a.offerId], 'expired', 'no funds verified → nothing to move');
  assert.equal(byRef[b.offerId], 'refund', 'funded + unanswered → refund');
  assert.equal(byRef[c.offerId], undefined, 'accepted still inside its join window');

  clock.tick(5 * 60 * 1000);             // past offerTtl + joinTtl
  actions = xo.sweep();
  assert.equal(actions.length, 1);
  assert.equal(actions[0].offer.offerId, c.offerId);
  assert.equal(actions[0].action, 'refund');
  assert.equal(c.endReason, 'never_joined');

  assert.equal(xo.sweep().length, 0, 'sweep is idempotent — refunding rows are not re-actioned');
});

test('cancelForRoom: room dies pre-session → funded offers refund, unfunded expire, ACTIVE untouched', () => {
  const { xo } = mk();
  const dead = xo.create({ ...TERMS, room: 'dying' }).offer;
  xo.fund(dead.offerId, 'cnoobz', 'tx1');
  const live = xo.create({ ...TERMS, inviter: 'other', room: 'dying', expert: 'noblemage' }).offer;
  xo.fund(live.offerId, 'other', 'tx2');
  xo.respond(live.offerId, 'noblemage', 'accept');
  xo.joined(live.offerId, 'noblemage', T0);

  const refunds = xo.cancelForRoom('dying', 'room_closed');
  assert.equal(refunds.length, 1, 'only the pre-session offer refunds');
  assert.equal(refunds[0].offerId, dead.offerId);
  assert.equal(live.status, 'active', 'active session is settled by the member-exit path, not cancelled');
});

test('findByExpert / activeByInviter lookups', () => {
  const { xo } = mk();
  const { offer } = xo.create(TERMS);
  xo.fund(offer.offerId, 'cnoobz', 'tx1');
  assert.equal(xo.activeByInviter('cnoobz').offerId, offer.offerId);
  assert.equal(xo.activeByInviter('guest33'), null);
  assert.equal(xo.findByExpert('guest33', 'warroom', ['offered']).offerId, offer.offerId);
  assert.equal(xo.findByExpert('guest33', 'warroom', ['active']), null);
  assert.equal(xo.findByExpert('guest33', 'otherroom', ['offered']), null);
});
