// Step-7 tests: the escrow-protocol/0.1 report·receipt seam (handover §6). Proves the
// node→escrow event-report and escrow→node settlement-receipt round-trips, signature
// tamper-detection, nonce dedup, and graceful degradation when no reporting key is
// available — i.e. the contract a Step-5 escrow box would speak over Nostr.

'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const escrowCore = require('escrow-core');
const { createEscrowReporter } = require('../escrow-report');

const TEST_SK = '11'.repeat(32);   // valid 32-byte schnorr secret (hex)
const reporterFor = (sk) => createEscrowReporter({ escrowCore, getSkHex: () => sk, reporter: () => 'v4call', service: 'v4call' });

test('event report: sign → accept round-trips (verified + fresh)', () => {
  const r = reporterFor(TEST_SK);
  const signed = r.buildSignedReport({ ref: 'call_1', subject: 'call_1', facts: { durationMs: 60000, endReason: 'hangup' }, nonce: r.settleNonce('call_1'), createdAt: 1000 });
  assert.ok(signed && signed.sig && signed.pubkey, 'report is signed');
  assert.equal(signed.proto, 'escrow-protocol/0.1');
  assert.equal(signed.reporter, 'v4call');
  const verdict = r.acceptReport(signed);
  assert.equal(verdict.verified, true);
  assert.equal(verdict.fresh, true);
  assert.equal(verdict.replay, false);
});

test('event report: the reporting pubkey matches escrow-core', () => {
  const r = reporterFor(TEST_SK);
  assert.equal(r.pubkey(), escrowCore.getReportingPubkey(TEST_SK));
});

test('event report: tampering the facts invalidates the signature', () => {
  const r = reporterFor(TEST_SK);
  const signed = r.buildSignedReport({ ref: 'call_2', facts: { durationMs: 60000 }, nonce: 'n2', createdAt: 1 });
  signed.facts = { durationMs: 99999999 };   // attacker inflates usage after signing
  const verdict = r.acceptReport(signed);
  assert.equal(verdict.verified, false);
});

test('event report: a wrong-key signature is rejected', () => {
  const signer = reporterFor('22'.repeat(32));
  const signed = signer.buildSignedReport({ ref: 'call_3', facts: {}, nonce: 'n3', createdAt: 1 });
  const verifier = reporterFor(TEST_SK);          // expects a different pubkey
  assert.equal(verifier.acceptReport(signed).verified, false);
});

test('event report: a repeated nonce is flagged as replay (the box drops it)', () => {
  const r = reporterFor(TEST_SK);
  const mk = () => r.buildSignedReport({ ref: 'call_4', facts: {}, nonce: r.settleNonce('call_4'), createdAt: 1 });
  assert.equal(r.acceptReport(mk()).fresh, true);
  const second = r.acceptReport(mk());
  assert.equal(second.fresh, false);
  assert.equal(second.replay, true);
});

test('settlement receipt: sign → verify round-trips; tamper is caught', () => {
  const r = reporterFor(TEST_SK);
  const receipt = r.buildSignedReceipt({ ref: 'call_5', settlement: 0.7, refund: 0.3, currency: 'HBD', disburseTx: 'tx_abc', status: 'settled', createdAt: 1 });
  assert.ok(receipt && receipt.sig);
  assert.equal(receipt.type, 'settlement-receipt');
  assert.equal(receipt.settlement, 0.7);
  assert.equal(r.verifyReceipt(receipt), true);
  receipt.settlement = 999;
  assert.equal(r.verifyReceipt(receipt), false);
});

test('settlement receipt: invalid status is rejected (build fails → null, non-throwing)', () => {
  const r = reporterFor(TEST_SK);
  const receipt = r.buildSignedReceipt({ ref: 'call_6', settlement: 1, refund: 0, currency: 'HBD', status: 'bogus', createdAt: 1 });
  assert.equal(receipt, null);
});

test('degraded (no reporting key yet): seam stays unsigned, never blocks', () => {
  const r = reporterFor(null);   // key file not ready
  assert.equal(r.buildSignedReport({ ref: 'c', facts: {}, nonce: 'n', createdAt: 1 }), null);
  assert.equal(r.pubkey(), null);
  // acceptReport(null) is the "no report" path — treated as fresh + unsigned, non-gating.
  const verdict = r.acceptReport(null);
  assert.equal(verdict.unsigned, true);
  assert.equal(verdict.fresh, true);
  // receipt is built UNSIGNED (bare canonical payload), and verifyReceipt is false on it.
  const receipt = r.buildSignedReceipt({ ref: 'c', settlement: 1, refund: 0, currency: 'HBD', status: 'settled', createdAt: 1 });
  assert.ok(receipt && !receipt.sig);
  assert.equal(r.verifyReceipt(receipt), false);
});
