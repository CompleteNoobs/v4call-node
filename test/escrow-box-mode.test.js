// ── test/escrow-box-mode.test.js — the node-side box-mode flip (ESCROW_MODE=box) ──
//
// Proves the KEYLESS node half of the escrow split end-to-end over a LOOPBACK transport (a fake
// client wired straight to a stub box), against the REAL queue + reporter + adapter:
//   1  settleCall builds the call-end envelope from durable rows, enqueues it durably, publishes
//      it, and finalizes (onSettled) EXACTLY once when the box's signed receipt arrives
//   2  the report envelope is well-formed and carries the combined-transfer re-split (callFacts)
//   3  a duplicate call-end is a no-op (queue UNIQUE(ref) — the box-mode single-winner guard);
//      the drainer republishes a still-pending report under the STABLE nonce (retry-until-received)
//   4  a receipt NOT signed by the pinned box key is rejected (verifyReceiptFromBox is the gate)
//
// The real box's settlement math + on-chain re-verification are proven in v4call-escrow/test;
// here the stub box just returns a canned signed receipt, so this exercises the NODE plumbing.

'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const os = require('os');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const escrowCore = require('escrow-core');
const { createEscrowReporter } = require('../escrow-report');
const { createSettlementQueue } = require('../escrow-settlement-queue');
const { createEscrowBoxMode } = require('../escrow-box-mode');

const NOW = 1_700_000_000_000;
const HALF_HOUR = 30 * 60 * 1000;

// One isolated harness per test: a node shadow ledger, a queue on it, a reporter, the box-mode
// glue with a LOOPBACK client factory, and a stub box whose response is controlled by `onReport`.
function harness({ onReport } = {}) {
  escrowCore.registerPrecision('HBD', 3);
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'v4call-boxmode-'));
  const adapter = escrowCore.createV4callAdapter({ account: 'tboxescrow', currency: 'HBD', keyEnv: 'TBOX_KEY' });
  const ledger = escrowCore.openLedger(path.join(dir, 'shadow.db'), { adapterMigrations: adapter.ledgerMigrations() });
  const queue = createSettlementQueue({ db: ledger.db });

  const nodeSk = crypto.randomBytes(32).toString('hex');
  const nodePub = escrowCore.getReportingPubkey(nodeSk);
  const boxSk = crypto.randomBytes(32).toString('hex');
  const boxPub = escrowCore.getReportingPubkey(boxSk);
  const reporter = createEscrowReporter({ escrowCore, getSkHex: () => nodeSk, reporter: () => 'tnode', service: 'v4call' });

  const published = [];
  let deliver = null;   // call to push a (signed) receipt back into the node

  // The loopback transport: publishReport acts as the box (verifies the report under the node key),
  // then — per the test's onReport — signs a receipt with the box key and delivers it to the node.
  function clientFactory() {
    let receiptCb = null;
    deliver = (signedReceipt) => receiptCb && receiptCb(signedReceipt);
    return {
      start(cb) { receiptCb = cb; },
      async publishReport(signed) {
        published.push(signed);
        assert.ok(escrowCore.verifyReport(signed, nodePub), 'box gate: report verifies under the node reporting key');
        const receiptObj = onReport ? onReport(signed) : defaultReceipt(signed);
        if (receiptObj) await receiptCb(escrowCore.signReport(receiptObj, boxSk));
      },
      close() {},
    };
  }

  const settledCalls = [];
  const boxMode = createEscrowBoxMode({
    escrowAdapter: adapter, escrowReporter: reporter, queue,
    boxPubkey: boxPub, relays: ['wss://x'], selfSkHex: () => nodeSk,
    maxDurationMin: 120, clientFactory,
    log: { log() {}, warn() {}, error() {} },
  });
  boxMode.onSettled((ref, ctx) => { settledCalls.push({ ref, ...ctx }); });

  return { dir, ledger, queue, reporter, nodeSk, nodePub, boxSk, boxPub, boxMode, published, settledCalls,
    deliverReceipt: (r) => deliver(r),
    cleanup() { try { ledger.close(); } catch {} try { fs.rmSync(dir, { recursive: true, force: true }); } catch {} },
  };
}

function defaultReceipt(signed) {
  // a plausible box outcome for the standard call (deposit 2.0, durationCost 1.0, refund 1.0)
  return escrowCore.buildSettlementReceipt({
    ref: signed.ref, settlement: 1.0, refund: 1.0, dust: 0, currency: 'HBD',
    disburseTx: 'boxtx_1', status: 'settled', createdAt: NOW });
}

// A combined-transfer call recorded into the node's shadow ledger: ONE deposit row carrying the
// non-refundable ring/connect as columns (the live node's shape → the box re-split path).
function recordCombinedCall(h, callId) {
  h.ledger.recordPayment({
    tx_id: `tx_${callId}`, ref: callId, sender: 'caller', currency: 'HBD', amount: 2.00,
    memo: `v4call:call:${callId}`, rate_per_hour: 2, start_ts: NOW - HALF_HOUR,
    platform_fee: 0.10, callee: 'callee', connect_paid: 0.05, ring_paid: 0.01,
  });
  return h.ledger.getPaymentsByRef(callId);
}

test('1+2 — settleCall builds + enqueues + publishes the envelope, finalizes once on the receipt', async () => {
  const h = harness();
  try {
    const payRows = recordCombinedCall(h, 'cA');
    const won = await h.boxMode.settleCall({ callId: 'cA', payRows, endReason: 'hangup', now: NOW });
    assert.equal(won, true, 'fresh call → enqueued');

    // enqueued + published exactly one report
    assert.equal(h.published.length, 1, 'one report published');
    const rep = h.published[0];
    assert.equal(rep.proto, 'escrow-protocol/0.1');
    assert.equal(rep.type, 'event-report');
    assert.equal(rep.ref, 'cA');
    assert.equal(rep.nonce, 'cA:settle', 'stable settle nonce');
    // (2) envelope carries the combined-transfer re-split + the payment
    assert.equal(rep.facts.payments.length, 1);
    assert.equal(rep.facts.callFacts.callee, 'callee');
    assert.equal(rep.facts.callFacts.connectPaid, 0.05, 'connect surfaced for the box re-split');
    assert.equal(rep.facts.callFacts.ringPaid, 0.01, 'ring surfaced for the box re-split');

    // finalized exactly once, from the box-signed receipt
    assert.equal(h.settledCalls.length, 1, 'onSettled fired once');
    const s = h.settledCalls[0];
    assert.equal(s.ref, 'cA');
    assert.equal(s.receipt.settlement, 1.0);
    assert.equal(s.receipt.refund, 1.0);
    assert.equal(s.facts.callFacts.callee, 'callee', 'finalize gets the reported facts for display');
    assert.ok(h.reporter.verifyReceiptFromBox(s.receipt, h.boxPub), 'receipt verifies under the pinned box key');

    // durable row is terminal
    const row = h.queue.get('cA');
    assert.equal(row.status, 'settled');
    assert.ok(row.receipt_json, 'receipt persisted');
  } finally { h.cleanup(); }
});

test('3 — duplicate call-end is a no-op; the drainer republishes a pending report under the stable nonce', async () => {
  // box stays silent (no receipt) so the report stays pending → we can exercise the guards.
  const h = harness({ onReport: () => null });
  try {
    const payRows = recordCombinedCall(h, 'cB');
    const first = await h.boxMode.settleCall({ callId: 'cB', payRows, endReason: 'hangup', now: NOW });
    assert.equal(first, true);
    assert.equal(h.published.length, 1, 'published once');

    // duplicate call-end: UNIQUE(ref) makes enqueue a no-op → no second publish
    const dup = await h.boxMode.settleCall({ callId: 'cB', payRows, endReason: 'hangup', now: NOW });
    assert.equal(dup, false, 'duplicate call-end ignored (single-winner)');
    assert.equal(h.published.length, 1, 'no double publish');

    // the drainer retries the still-pending report — same stable nonce (box dedups), no new row
    await h.boxMode.drainOnce();
    assert.equal(h.published.length, 2, 'drainer republished');
    assert.equal(h.published[1].nonce, h.published[0].nonce, 'stable nonce across retries');
    assert.equal(h.settledCalls.length, 0, 'still unsettled (box never answered)');

    // now the box finally answers → finalize exactly once
    await h.deliverReceipt(escrowCore.signReport(defaultReceipt({ ref: 'cB' }), h.boxSk));
    assert.equal(h.settledCalls.length, 1, 'finalized once when the receipt finally arrives');
    assert.equal(h.queue.get('cB').status, 'settled');
  } finally { h.cleanup(); }
});

test('4 — a receipt NOT signed by the pinned box key is rejected (no finalize)', async () => {
  const h = harness({ onReport: () => null });   // we deliver receipts by hand
  try {
    const payRows = recordCombinedCall(h, 'cC');
    await h.boxMode.settleCall({ callId: 'cC', payRows, endReason: 'hangup', now: NOW });

    // a receipt signed by an IMPOSTOR key (not the pinned box key) must be dropped
    const impostorSk = crypto.randomBytes(32).toString('hex');
    await h.deliverReceipt(escrowCore.signReport(defaultReceipt({ ref: 'cC' }), impostorSk));
    assert.equal(h.settledCalls.length, 0, 'impostor receipt rejected — not finalized');
    assert.equal(h.queue.get('cC').status, 'pending', 'row stays pending');

    // the genuine box receipt is accepted
    await h.deliverReceipt(escrowCore.signReport(defaultReceipt({ ref: 'cC' }), h.boxSk));
    assert.equal(h.settledCalls.length, 1, 'genuine receipt finalizes');
  } finally { h.cleanup(); }
});
