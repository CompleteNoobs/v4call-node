// Step-5 tests: the escrow-core.disburse swap that now backs sendFromEscrow*.
// Proves the two properties v4call's wrappers + finalizeOutflow rely on:
//   1. a missing key env throws code:'no_key' (→ refund row left PENDING, never failed)
//   2. per-currency precision op building (native 3dp; HE token at the locked `places`).
// disburse's broadcast itself is covered by escrow-core's own suite; here we pin the
// contract v4call depends on.

'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const escrowCore = require('escrow-core');
const { buildDisburseOp } = escrowCore.modules.sign;

test('disburse: unset key env throws code:no_key (→ caller leaves refund PENDING)', async () => {
  // __V4CALL_TEST_UNSET_KEY__ is never set → disburse must throw no_key BEFORE any network.
  await assert.rejects(
    escrowCore.disburse({ to: 'bob', amount: 1, currency: 'HBD', memo: 'm', fromAccount: 'v4call-escrow', keyEnv: '__V4CALL_TEST_UNSET_KEY__' }),
    (e) => e.code === 'no_key',
  );
});

test('disburse: missing keyEnv name throws bad_request', async () => {
  await assert.rejects(
    escrowCore.disburse({ to: 'bob', amount: 1, currency: 'HBD', memo: 'm', fromAccount: 'v4call-escrow' }),
    (e) => e.code === 'bad_request',
  );
});

test('buildDisburseOp: native HBD/HIVE formats at 3dp, lowercases accounts', () => {
  const op = buildDisburseOp({ to: 'Bob', amount: 1.5, currency: 'HBD', memo: 'v4call:payout:c1', fromAccount: 'v4call-escrow' });
  assert.equal(op[0], 'transfer');
  assert.equal(op[1].amount, '1.500 HBD');
  assert.equal(op[1].to, 'bob');
  assert.equal(op[1].from, 'v4call-escrow');
  assert.equal(op[1].memo, 'v4call:payout:c1');
});

test('buildDisburseOp: HE token formats quantity at its locked precision (places)', () => {
  const op = buildDisburseOp({ to: 'bob', amount: 1.5, currency: 'CNOOBS', memo: 'm', fromAccount: 'v4call-escrow', places: 8 });
  assert.equal(op[0], 'custom_json');
  assert.deepEqual(op[1].required_auths, ['v4call-escrow']);
  assert.equal(op[1].id, 'ssc-mainnet-hive');
  const payload = JSON.parse(op[1].json).contractPayload;
  assert.equal(payload.symbol, 'CNOOBS');
  assert.equal(payload.quantity, '1.50000000');   // 8dp, not the old 3dp native default
  assert.equal(payload.to, 'bob');
});

test('buildDisburseOp: rejects bad amount / destination', () => {
  assert.throws(() => buildDisburseOp({ to: 'bob', amount: 0, currency: 'HBD', fromAccount: 'v4call-escrow' }), /amount/);
  assert.throws(() => buildDisburseOp({ to: 'bad name!', amount: 1, currency: 'HBD', fromAccount: 'v4call-escrow' }), /destination/);
});
