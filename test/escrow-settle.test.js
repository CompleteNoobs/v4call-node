// Step-3 settlement tests (handover-v4call-escrow-migration.md §7) — the money-safety
// properties processCallEnd now relies on: the settle() cap, per-currency precision,
// the single-winner atomicClose flip, crash-mid-settle (no double-disburse), and the
// multi-row deposit-cap aggregation (deposit + top-ups). Exercised against escrow-core
// + a REAL temp ledger, the same way processCallEnd uses them.

'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const os = require('os');
const path = require('path');
const fs = require('fs');

const escrowCore = require('escrow-core');

function freshLedger() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'es-'));
  const adapter = escrowCore.createV4callAdapter();
  const ledger = escrowCore.openLedger(path.join(dir, 't.db'), { adapterMigrations: adapter.ledgerMigrations() });
  return { ledger, adapter, cleanup: () => { ledger.close(); fs.rmSync(dir, { recursive: true, force: true }); } };
}

// ── settle(): the cap invariant ───────────────────────────────────────────────

test('settle: over-long call caps at deposit, refund 0 (min(usage,deposit))', () => {
  const { settlement, refund } = escrowCore.settle({ deposit: 1.0, meteredUsage: 5.0, currency: 'HBD', places: 3, dustFloor: 0.001 });
  assert.equal(settlement, 1.0);
  assert.equal(refund, 0);
});

test('settle: partial usage → pro-rata settlement + refund', () => {
  const { settlement, refund } = escrowCore.settle({ deposit: 1.0, meteredUsage: 0.3, currency: 'HBD', places: 3, dustFloor: 0.001 });
  assert.equal(settlement, 0.3);
  assert.equal(refund, 0.7);
});

test('settle: a wrong/over-stated usage can never mint money beyond the deposit', () => {
  // The whole point of the cap: an inflated event report only re-splits the verified
  // envelope, it can't disburse more than was escrowed.
  const { settlement, refund } = escrowCore.settle({ deposit: 0.5, meteredUsage: 999, currency: 'HBD', places: 3 });
  assert.equal(settlement + refund, 0.5);
  assert.equal(settlement, 0.5);
});

// ── precision (Decision #3) ───────────────────────────────────────────────────

test('settle: HBD/HIVE round at 3dp', () => {
  const { settlement } = escrowCore.settle({ deposit: 5, meteredUsage: 1.23456, currency: 'HBD', places: 3 });
  assert.equal(settlement, 1.235);
});

test('settle: a 0-dp token rounds at its locked precision', () => {
  const { settlement } = escrowCore.settle({ deposit: 5, meteredUsage: 1.7, currency: 'WHOLECOIN', places: 0 });
  assert.equal(settlement, 2);
});

// ── atomicClose: single-winner flip ───────────────────────────────────────────

test('atomicClose: first call-end wins, a duplicate loses (no double-disburse)', () => {
  const { ledger, cleanup } = freshLedger();
  ledger.recordPayment({ tx_id: 't1', ref: 'call_1', sender: 'alice', currency: 'HBD', amount: 1, memo: 'm' });
  assert.equal(ledger.atomicClose('call_1'), true);   // the settling call-end
  assert.equal(ledger.atomicClose('call_1'), false);  // a duplicate / crash-retry call-end
  cleanup();
});

test('atomicClose: closing a ref with no open row returns false (legacy/in-flight → skip)', () => {
  const { ledger, cleanup } = freshLedger();
  assert.equal(ledger.atomicClose('never-seen'), false);
  cleanup();
});

// ── crash mid-settle: closed payment + pending refund survives; retry never doubles ──

test('crash mid-settle: pending refund survives; retry settles once, no double-disburse', () => {
  const { ledger, cleanup } = freshLedger();
  ledger.recordPayment({ tx_id: 't1', ref: 'call_1', sender: 'alice', currency: 'HBD', amount: 1, memo: 'm' });

  // settle wins, refund recorded pending — then the process CRASHES before broadcast.
  assert.equal(ledger.atomicClose('call_1'), true);
  const { refund_id } = ledger.recordRefund({ ref: 'call_1', to_account: 'alice', amount: 0.7, currency: 'HBD', memo: 'r', reason: 'refund' });
  assert.equal(ledger.getRefund(refund_id).status, 'pending');

  // --- restart: a duplicate call-end arrives ---
  assert.equal(ledger.atomicClose('call_1'), false, 'already closed → processCallEnd would SKIP disburse');

  // the single pending refund row is intact (not re-created, not double-sent)
  const refunds = ledger.db.prepare("SELECT * FROM refunds WHERE ref = 'call_1'").all();
  assert.equal(refunds.length, 1);
  assert.equal(refunds[0].status, 'pending');

  // a retry completes it exactly once
  ledger.markRefundSettled(refund_id, 'sent', 'disburse_tx_1');
  assert.equal(ledger.getRefund(refund_id).status, 'sent');
  assert.equal(ledger.getRefund(refund_id).tx_id, 'disburse_tx_1');
  cleanup();
});

test('refund lifecycle: pending → skipped (dust) and pending → failed are both terminal-recordable', () => {
  const { ledger, cleanup } = freshLedger();
  ledger.recordPayment({ tx_id: 't1', ref: 'call_1', sender: 'a', currency: 'HBD', amount: 1, memo: 'm' });
  const a = ledger.recordRefund({ ref: 'call_1', to_account: 'a', amount: 0.0001, currency: 'HBD', memo: 'dust', reason: 'refund' });
  const b = ledger.recordRefund({ ref: 'call_1', to_account: 'srv', amount: 0.1, currency: 'HBD', memo: 'fee', reason: 'platform_fee' });
  ledger.markRefundSettled(a.refund_id, 'skipped', null);
  ledger.markRefundSettled(b.refund_id, 'failed', null);
  assert.equal(ledger.getRefund(a.refund_id).status, 'skipped');
  assert.equal(ledger.getRefund(b.refund_id).status, 'failed');
  cleanup();
});

// ── deposit cap = sum of deposit + top-up rows (processCallEnd aggregation) ────

test('deposit cap = sum of all rows for the ref; primary row carries the locked facts', () => {
  const { ledger, cleanup } = freshLedger();
  // The single consolidated deposit row (Step-2 model) + a later top-up row, same ref.
  ledger.recordPayment({ tx_id: 't_dep', ref: 'call_1', sender: 'alice', currency: 'HBD', amount: 0.5, memo: 'v4call:pay:call_1:bob', connect_paid: 0.3, ring_paid: 0.2, rate_per_hour: 6, platform_fee: 0.1, callee: 'bob' });
  ledger.recordPayment({ tx_id: 't_top', ref: 'call_1', sender: 'alice', currency: 'HBD', amount: 0.4, memo: 'v4call:topup:call_1', callee: 'bob' });

  const rows = ledger.getPaymentsByRef('call_1');
  const depositCap = rows.reduce((s, r) => s + (Number(r.amount) || 0), 0);
  assert.equal(depositCap, 0.9, 'deposit (0.5) + top-up (0.4)');

  const primary = rows.find(r => r.rate_per_hour != null || r.ring_paid != null || r.connect_paid != null);
  assert.equal(primary.ring_paid, 0.2);
  assert.equal(primary.connect_paid, 0.3);
  assert.equal(primary.rate_per_hour, 6);
  assert.equal(primary.platform_fee, 0.1);
  assert.equal(primary.callee, 'bob');

  // End-to-end: settle the summed cap against a metered usage that exceeds it → caps.
  const { settlement, refund } = escrowCore.settle({ deposit: depositCap, meteredUsage: 2.0, currency: 'HBD', places: 3 });
  assert.equal(settlement, 0.9);
  assert.equal(refund, 0);
  cleanup();
});

// ── adapter.meteredUsage: the metering seam processCallEnd feeds settle() ──────

test('meteredUsage: rate × elapsed hours, clamped to max duration', () => {
  const { adapter, cleanup } = freshLedger();
  const start = 1_000_000;
  // 30 min elapsed at 6/hr → 3.0
  assert.equal(adapter.meteredUsage({ rate_per_hour: 6, start_ts: start }, start + 30 * 60 * 1000), 3);
  // 5 hours elapsed but max_duration_min=120 (2h) → clamp to 2h × 6 = 12
  assert.equal(adapter.meteredUsage({ rate_per_hour: 6, start_ts: start, max_duration_min: 120 }, start + 5 * 3600 * 1000), 12);
  // no rate or no start → 0
  assert.equal(adapter.meteredUsage({ rate_per_hour: 0, start_ts: start }, start + 3600 * 1000), 0);
  cleanup();
});

test('Step 6: start_ts persists onto the durable row and is the metering authority', () => {
  // recordEscrowStartTs writes the connect time onto the row at answer-time; settlement
  // then reads start_ts from the ROW (not activePayments) — the row is self-sufficient.
  const { ledger, adapter, cleanup } = freshLedger();
  ledger.recordPayment({ tx_id: 't_dep', ref: 'call_1', sender: 'alice', currency: 'HBD', amount: 1.0, memo: 'm', rate_per_hour: 6, connect_paid: 0.3, ring_paid: 0.2, callee: 'bob' });

  const connectedAt = 5_000_000;
  ledger.db.prepare('UPDATE payments SET start_ts = ? WHERE ref = ?').run(connectedAt, 'call_1');   // recordEscrowStartTs

  const primary = ledger.getPaymentsByRef('call_1').find(r => r.rate_per_hour != null);
  assert.equal(primary.start_ts, connectedAt, 'start_ts is on the durable row');

  const now = connectedAt + 30 * 60 * 1000;   // 30 min call
  const usage = adapter.meteredUsage({ rate_per_hour: primary.rate_per_hour, start_ts: primary.start_ts, max_duration_min: 120 }, now);
  assert.equal(usage, 3);                       // 0.5h × 6 — driven entirely by the row
  const { settlement, refund } = escrowCore.settle({ deposit: primary.amount, meteredUsage: usage, currency: 'HBD', places: 3 });
  assert.equal(settlement, 1.0);                // capped at the deposit
  assert.equal(refund, 0);
  cleanup();
});

test('federated metering: duration-from-message via synthesized start (now - durationMs)', () => {
  // processFederatedCallEnd has no local clock — the duration arrives in the call-ended
  // message. It synthesizes start_ts = now - durationMs so the SAME metering seam yields
  // rate × hours, then settle() caps at the deposit.
  const { adapter, cleanup } = freshLedger();
  const now = 9_000_000;
  const durationMs = 30 * 60 * 1000;            // 30 min
  const usage = adapter.meteredUsage({ rate_per_hour: 6, start_ts: now - durationMs, max_duration_min: 120 }, now);
  assert.equal(usage, 3);                        // 0.5h × 6
  const { settlement, refund } = escrowCore.settle({ deposit: 2.0, meteredUsage: usage, currency: 'HBD', places: 3 });
  assert.equal(settlement, 2.0);                 // capped at deposit
  assert.equal(refund, 0);
  cleanup();
});
