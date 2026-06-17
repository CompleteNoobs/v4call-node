// Step-4 escrow-migration money-path tests (handover-v4call-escrow-migration.md §7).
//
// Two layers:
//  1. verifyAndRecordPayment orchestration (the seam server.js wires into every paid
//     handler) — driven with a MOCK escrowCore + a REAL temp escrow-core ledger, so the
//     replay guard, require-txId, sidechain gating, verify-only mode, and the v4call
//     single-row call model (ring verify-only + deposit records one row) are all real.
//  2. escrowCore.verifyPayment itself — tx_id-anchored + EXACT-memo, with an injected
//     getTransaction, proving the on-chain checks every paid path now relies on.

'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const os = require('os');
const path = require('path');
const fs = require('fs');

const escrowCore = require('escrow-core');
const { createEscrowVerify } = require('../escrow-verify');

// ── helpers ──────────────────────────────────────────────────────────────────
function freshLedger() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ev-'));
  const adapter = escrowCore.createV4callAdapter();
  const ledger = escrowCore.openLedger(path.join(dir, 't.db'), { adapterMigrations: adapter.ledgerMigrations() });
  return { ledger, dir, cleanup: () => { ledger.close(); fs.rmSync(dir, { recursive: true, force: true }); } };
}

// Mock escrowCore: verifyPayment echoes inputs as a confirmed payment by default;
// `verifyThrows` simulates an on-chain mismatch; `sidechain` controls HE confirm.
function mockCore({ verifyThrows = null, sidechain = { confirmed: true }, paid = null } = {}) {
  return {
    isNativeCurrency: (c) => ['HIVE', 'HBD'].includes(String(c).toUpperCase()),
    async verifyPayment({ txId, sender, currency, expectedAmount }) {
      if (verifyThrows) throw Object.assign(new Error(verifyThrows), { code: 'unprocessable_entity' });
      return { txId, sender: String(sender).toLowerCase(), paid: paid != null ? paid : expectedAmount, currency, blockNum: 4242, confirmed: true };
    },
    async verifySidechain() { return sidechain; },
  };
}

const base = { sender: 'Alice', escrowAccount: 'v4call-escrow', currency: 'HBD', memo: 'v4call:text:m1', expectedAmount: 1 };

// ── 1. verifyAndRecordPayment orchestration ──────────────────────────────────

test('requires a txId (owner decision: no scan fallback)', async () => {
  const { ledger, cleanup } = freshLedger();
  const { verifyAndRecordPayment } = createEscrowVerify({ escrowCore: mockCore(), escrowLedger: ledger });
  const res = await verifyAndRecordPayment({ ...base, ref: 'm1' });  // no txId
  assert.equal(res.ok, false);
  assert.match(res.reason, /transaction id/i);
  assert.equal(ledger.getPaymentsByRef('m1').length, 0);
  cleanup();
});

test('happy path: verifies + writes a durable row (sender lowercased, block captured)', async () => {
  const { ledger, cleanup } = freshLedger();
  const { verifyAndRecordPayment } = createEscrowVerify({ escrowCore: mockCore(), escrowLedger: ledger });
  const res = await verifyAndRecordPayment({ ...base, txId: 'tx1', ref: 'm1', cols: { callee: 'bob' } });
  assert.equal(res.ok, true);
  assert.equal(res.paid, 1);
  assert.equal(res.blockNum, 4242);
  const row = ledger.getPaymentByTxId('tx1');
  assert.ok(row);
  assert.equal(row.ref, 'm1');
  assert.equal(row.sender, 'alice');
  assert.equal(row.amount, 1);
  assert.equal(row.callee, 'bob');
  assert.equal(row.block_num, 4242);
  cleanup();
});

test('replay: same txId twice → second rejected, only one row', async () => {
  const { ledger, cleanup } = freshLedger();
  const { verifyAndRecordPayment } = createEscrowVerify({ escrowCore: mockCore(), escrowLedger: ledger });
  const r1 = await verifyAndRecordPayment({ ...base, txId: 'dup', ref: 'm1' });
  const r2 = await verifyAndRecordPayment({ ...base, txId: 'dup', ref: 'm1' });
  assert.equal(r1.ok, true);
  assert.equal(r2.ok, false);
  assert.equal(r2.replay, true);
  assert.equal(ledger.getPaymentsByRef('m1').length, 1);
  cleanup();
});

test('replay across refs: a txId used for one ref cannot be reused for another', async () => {
  const { ledger, cleanup } = freshLedger();
  const { verifyAndRecordPayment } = createEscrowVerify({ escrowCore: mockCore(), escrowLedger: ledger });
  const r1 = await verifyAndRecordPayment({ ...base, txId: 'dup', ref: 'call_1' });
  const r2 = await verifyAndRecordPayment({ ...base, txId: 'dup', ref: 'invite_9' }); // attacker reuse
  assert.equal(r1.ok, true);
  assert.equal(r2.ok, false);
  assert.equal(r2.replay, true);
  assert.equal(ledger.getPaymentsByRef('invite_9').length, 0);
  cleanup();
});

test('on-chain mismatch (bad memo/amount/account) → not recorded', async () => {
  const { ledger, cleanup } = freshLedger();
  const core = mockCore({ verifyThrows: 'memo mismatch' });
  const { verifyAndRecordPayment } = createEscrowVerify({ escrowCore: core, escrowLedger: ledger });
  const res = await verifyAndRecordPayment({ ...base, txId: 'tx1', ref: 'm1' });
  assert.equal(res.ok, false);
  assert.match(res.reason, /not valid on chain/i);
  assert.equal(ledger.getPaymentByTxId('tx1'), undefined);
  cleanup();
});

test('HE token: sidechain rejected → not recorded; confirmed → recorded', async () => {
  const { ledger, cleanup } = freshLedger();
  const rejected = createEscrowVerify({ escrowCore: mockCore({ sidechain: { confirmed: false, reason: 'rejected' } }), escrowLedger: ledger });
  const r1 = await rejected.verifyAndRecordPayment({ ...base, currency: 'CNOOBS', txId: 'tx_he_1', ref: 'm1' });
  assert.equal(r1.ok, false);
  assert.match(r1.reason, /Hive-Engine/);
  assert.equal(ledger.getPaymentByTxId('tx_he_1'), undefined);

  const confirmed = createEscrowVerify({ escrowCore: mockCore({ sidechain: { confirmed: true } }), escrowLedger: ledger });
  const r2 = await confirmed.verifyAndRecordPayment({ ...base, currency: 'CNOOBS', txId: 'tx_he_2', ref: 'm2' });
  assert.equal(r2.ok, true);
  assert.ok(ledger.getPaymentByTxId('tx_he_2'));
  cleanup();
});

test('verify-only (record:false): confirms on chain but writes NO row', async () => {
  const { ledger, cleanup } = freshLedger();
  const { verifyAndRecordPayment } = createEscrowVerify({ escrowCore: mockCore(), escrowLedger: ledger });
  const res = await verifyAndRecordPayment({ ...base, txId: 'tx1', ref: 'inv1', record: false });
  assert.equal(res.ok, true);
  assert.equal(ledger.getPaymentByTxId('tx1'), undefined);
  assert.equal(ledger.getPaymentsByRef('inv1').length, 0);
  cleanup();
});

test('undefined adapter cols are dropped (no better-sqlite3 bind crash)', async () => {
  const { ledger, cleanup } = freshLedger();
  const { verifyAndRecordPayment } = createEscrowVerify({ escrowCore: mockCore(), escrowLedger: ledger });
  const res = await verifyAndRecordPayment({ ...base, txId: 'tx1', ref: 'm1', cols: { rate_per_hour: undefined, callee: 'bob' } });
  assert.equal(res.ok, true);
  const row = ledger.getPaymentByTxId('tx1');
  assert.equal(row.rate_per_hour, null);   // absent → NULL, not a crash
  assert.equal(row.callee, 'bob');
  cleanup();
});

test('v4call call model: ring verify-only + deposit writes ONE consolidated row', async () => {
  const { ledger, cleanup } = freshLedger();
  const { verifyAndRecordPayment } = createEscrowVerify({ escrowCore: mockCore(), escrowLedger: ledger });
  const callId = 'call_1', txId = 'tx_call_1', memo = 'v4call:pay:call_1:bob';
  // ONE on-chain transfer of total=1.0 (ring 0.2 + connect 0.3 + deposit 0.5).
  // Ring is verify-only (shares the deposit's txId) → no row.
  const ring = await verifyAndRecordPayment({
    sender: 'alice', escrowAccount: 'v4call-escrow', currency: 'HBD', memo,
    txId, ref: callId, expectedAmount: 0.2, record: false,
  });
  assert.equal(ring.ok, true);
  assert.equal(ledger.getPaymentsByRef(callId).length, 0);

  // Deposit verifies the FULL transfer (>=1.0) but stores amount = deposit cap (0.5),
  // with the non-refundable portions + locked rate as adapter columns.
  const dep = await verifyAndRecordPayment({
    sender: 'alice', escrowAccount: 'v4call-escrow', currency: 'HBD', memo,
    txId, ref: callId, expectedAmount: 1.0, recordAmount: 0.5,
    cols: { connect_paid: 0.3, ring_paid: 0.2, rate_per_hour: 6, platform_fee: 0.1, callee: 'bob' },
  });
  assert.equal(dep.ok, true);

  const rows = ledger.getPaymentsByRef(callId);
  assert.equal(rows.length, 1, 'one transfer → one durable row (no tx_id collision)');
  assert.equal(rows[0].amount, 0.5, 'amount = refundable deposit cap');
  assert.equal(rows[0].ring_paid, 0.2);
  assert.equal(rows[0].connect_paid, 0.3);
  assert.equal(rows[0].rate_per_hour, 6);
  assert.equal(rows[0].platform_fee, 0.1);
  assert.equal(rows[0].callee, 'bob');

  // Redelivery of the deposit (same txId) is rejected as replay.
  const again = await verifyAndRecordPayment({
    sender: 'alice', escrowAccount: 'v4call-escrow', currency: 'HBD', memo,
    txId, ref: callId, expectedAmount: 1.0, recordAmount: 0.5, cols: { callee: 'bob' },
  });
  assert.equal(again.ok, false);
  assert.equal(again.replay, true);
  assert.equal(ledger.getPaymentsByRef(callId).length, 1);
  cleanup();
});

// ── 2. escrowCore.verifyPayment — tx_id-anchored + EXACT-memo (real, injected tx) ──

const nativeTx = (over = {}) => ({
  block_num: 99,
  operations: [['transfer', {
    from: 'alice', to: 'v4call-escrow', amount: '1.000 HBD', memo: 'v4call:pay:c1:bob', ...over,
  }]],
});
const verifyArgs = (over = {}) => ({
  txId: 'tx1', sender: 'alice', account: 'v4call-escrow', currency: 'HBD',
  expectedMemo: 'v4call:pay:c1:bob', expectedAmount: 1, ...over,
});

test('verifyPayment: native happy path (tx-anchored, captures block)', async () => {
  const tx = nativeTx();
  const res = await escrowCore.verifyPayment(verifyArgs(), { getTransaction: async () => tx });
  assert.equal(res.paid, 1);
  assert.equal(res.blockNum, 99);
  assert.equal(res.confirmed, true);
});

test('verifyPayment: EXACT-memo mismatch is rejected (stricter than the old substring scan)', async () => {
  const tx = nativeTx({ memo: 'v4call:pay:c1:bob EXTRA' });
  await assert.rejects(
    escrowCore.verifyPayment(verifyArgs(), { getTransaction: async () => tx }),
    /memo mismatch/i,
  );
});

test('verifyPayment: underpaid is rejected', async () => {
  const tx = nativeTx({ amount: '0.500 HBD' });
  await assert.rejects(
    escrowCore.verifyPayment(verifyArgs(), { getTransaction: async () => tx }),
    /underpaid/i,
  );
});

test('verifyPayment: wrong destination account is rejected', async () => {
  const tx = nativeTx({ to: 'attacker' });
  await assert.rejects(
    escrowCore.verifyPayment(verifyArgs(), { getTransaction: async () => tx }),
    /wrong account/i,
  );
});

test('verifyPayment: missing txId throws bad_request (require txId at the core)', async () => {
  await assert.rejects(
    escrowCore.verifyPayment(verifyArgs({ txId: undefined }), { getTransaction: async () => nativeTx() }),
    /required/i,
  );
});
