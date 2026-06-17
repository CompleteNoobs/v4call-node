// escrow-verify.js — the v4call ⇄ escrow-core verify+record seam (Step-4 migration).
//
// Factory wrapper around escrow-core so the money-path verify logic is unit-testable
// in isolation (server.js is a monolith that self-starts on require). server.js calls
// createEscrowVerify({ escrowCore, escrowLedger }) once at boot; tests call it with a
// mock escrowCore + a REAL temp escrow-core ledger to exercise the replay guard,
// require-txId, sidechain gating, the single-row call model, and verify-only mode.
//
// The Step-4 money-safety upgrade (handover-v4call-escrow-migration.md §2–§4): every
// paid path verifies the SPECIFIC broadcast tx via escrowCore.verifyPayment (tx_id-
// anchored + EXACT-memo) instead of scanning account history / balances, and writes a
// durable, replay-guarded row to the escrow-core ledger (tx_id UNIQUE). The client MUST
// supply the Keychain txId — owner decision: require txId, NO legacy scan fallback.
// HE-token txs are additionally hard-confirmed on the Hive-Engine sidechain (a Hive-
// layer broadcast succeeding ≠ the token transfer was accepted).

'use strict';

function createEscrowVerify({ escrowCore, escrowLedger }) {
  if (!escrowCore || !escrowLedger) {
    throw new Error('createEscrowVerify: escrowCore and escrowLedger are required');
  }

  /**
   * Verify an on-chain payment (tx-anchored) and durably record it (replay-guarded).
   *
   * Returns { ok, reason, replay, txId, paid, blockNum }. On ok:false the caller
   * surfaces `reason` to the user the same way the old verify-failure messages did.
   * `recordAmount` overrides the stored `amount` (the settlement cap) when it differs
   * from the on-chain transfer total (e.g. a deposit tx pays connect+deposit but only
   * the deposit portion is the refundable cap). `cols` are adapter columns to lock onto
   * the row (rate_per_hour, start_ts, connect_paid, ring_paid, platform_fee, callee).
   * `record:false` → verify-only (tx-anchored + sidechain hard-confirm) WITHOUT a
   * durable row — used where this server validates a payment it neither holds nor
   * settles (the inviter-holds-funds federated room-invite: the funds + the replay-
   * guarded row live on the inviter's server, not ours).
   */
  async function verifyAndRecordPayment({
    txId, sender, escrowAccount, currency, memo, expectedAmount,
    ref, recordAmount = null, cols = {}, record = true,
  }) {
    if (!txId || typeof txId !== 'string') {
      return { ok: false, reason: 'Missing payment transaction id — update your client to a version that reports the payment tx.' };
    }
    // Replay short-circuit: reject an already-recorded tx before any RPC.
    if (record && escrowLedger.getPaymentByTxId(txId)) {
      return { ok: false, replay: true, reason: 'This payment transaction has already been used.' };
    }

    let v;
    try {
      v = await escrowCore.verifyPayment({
        txId, sender, account: escrowAccount, currency, expectedMemo: memo, expectedAmount,
      });
    } catch (e) {
      console.warn(`[escrow] verify failed tx=${txId} @${sender}→@${escrowAccount} ${expectedAmount} ${currency}: ${e.message}`);
      return { ok: false, reason: `Payment not valid on chain: ${e.message}` };
    }

    // Hive-Engine token: hard-confirm the sidechain accepted the wrapped transfer.
    if (!escrowCore.isNativeCurrency(currency)) {
      try {
        const sc = await escrowCore.verifySidechain(txId);
        if (!sc.confirmed) {
          console.warn(`[escrow] sidechain ${sc.reason} tx=${txId} ${currency}`);
          return { ok: false, reason: `Token payment ${sc.reason === 'pending' ? 'not yet confirmed' : 'rejected'} on Hive-Engine.` };
        }
      } catch (e) {
        console.warn(`[escrow] sidechain confirm error tx=${txId}: ${e.message}`);
        return { ok: false, reason: `Could not confirm token payment on Hive-Engine: ${e.message}` };
      }
    }

    // Verify-only mode: payment confirmed on chain, but this server doesn't hold/
    // settle it, so no durable row (and no replay guard — that lives where the
    // funds + settlement live).
    if (!record) {
      return { ok: true, txId, paid: v.paid, blockNum: v.blockNum };
    }

    // Drop undefined adapter cols (better-sqlite3 rejects undefined binds; NULL is fine).
    const cleanCols = {};
    for (const [k, val] of Object.entries(cols)) if (val !== undefined) cleanCols[k] = val;

    try {
      escrowLedger.recordPayment({
        tx_id:     txId,
        ref,
        sender:    v.sender,
        currency:  v.currency,
        amount:    recordAmount != null ? recordAmount : v.paid,
        memo,
        block_num: v.blockNum,
        ...cleanCols,
      });
    } catch (e) {
      if (e.code === 'conflict') {
        return { ok: false, replay: true, reason: 'This payment transaction has already been used.' };
      }
      console.error(`[escrow] recordPayment failed tx=${txId} ref=${ref}: ${e.message}`);
      return { ok: false, reason: `Could not record payment: ${e.message}` };
    }

    console.log(`[escrow] ✓ verified+recorded ${v.currency} tx=${txId} ref=${ref} @${v.sender}→@${escrowAccount} paid=${v.paid} (block ${v.blockNum})`);
    return { ok: true, txId, paid: v.paid, blockNum: v.blockNum };
  }

  return { verifyAndRecordPayment };
}

module.exports = { createEscrowVerify };
