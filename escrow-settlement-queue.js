// ── v4call-node/escrow-settlement-queue.js — the durable node→box report queue (box mode) ──
//
// In box mode the node does NOT settle locally; it hands a call-end `event-report` to the escrow
// box and waits for a box-signed `settlement-receipt`. This table makes that hand-off DURABLE: a
// report's facts are persisted BEFORE it's published and only marked settled when a verified
// receipt arrives. A slow/unreachable box or a node restart therefore never loses a settlement —
// a drainer (escrow-box-mode.js) republishes pending rows (re-signing under the stable
// `ref:settle` nonce, which the box dedups) until the receipt lands ("retry until received").
//
// UNIQUE(ref) (PRIMARY KEY) is box mode's SINGLE-WINNER guard — it REPLACES the in-process
// atomicClose: a duplicate call-end's `INSERT OR IGNORE` is a no-op, so exactly one report is
// ever enqueued per call, and markSettled(pending→settled) fires finalization exactly once.
//
// Storage: the node's escrow SHADOW DB (v4call-escrow.db), created via the ledger's db handle —
// the same pattern recordEscrowStartTs uses (server.js). Node-scoped (NOT via the shared adapter
// migration) so the box's own ledger schema stays clean. We store the UNSIGNED facts (not a
// signed report): the drainer signs fresh at publish time, so a report enqueued before the
// reporting key is readable still settles once the key is available.

'use strict';

function createSettlementQueue({ db, log = () => {} }) {
  if (!db || typeof db.prepare !== 'function') throw new Error('createSettlementQueue: a better-sqlite3 db handle is required');

  db.exec(`
    CREATE TABLE IF NOT EXISTS pending_reports (
      ref          TEXT    PRIMARY KEY,          -- callId; UNIQUE = box-mode single-winner guard
      facts_json   TEXT    NOT NULL,             -- the call-end report FACTS (unsigned; signed at publish time)
      nonce        TEXT    NOT NULL,             -- stable one-shot ('ref:settle') so the box dedups republishes
      meta_json    TEXT,                         -- node-side finalize context (e.g. { callerServer, federated })
      status       TEXT    NOT NULL DEFAULT 'pending',   -- 'pending' | 'settled' | 'failed'
      created_at   INTEGER NOT NULL,             -- call-end epoch ms (also the report createdAt)
      last_attempt INTEGER,
      attempts     INTEGER NOT NULL DEFAULT 0,
      receipt_json TEXT                          -- the box-signed settlement-receipt, once settled
    );
  `);

  const _insert      = db.prepare(`INSERT OR IGNORE INTO pending_reports (ref, facts_json, nonce, meta_json, status, created_at) VALUES (?, ?, ?, ?, 'pending', ?)`);
  const _pending     = db.prepare(`SELECT * FROM pending_reports WHERE status = 'pending' ORDER BY created_at ASC`);
  const _get         = db.prepare(`SELECT * FROM pending_reports WHERE ref = ?`);
  const _markAttempt = db.prepare(`UPDATE pending_reports SET last_attempt = ?, attempts = attempts + 1 WHERE ref = ?`);
  const _markSettled = db.prepare(`UPDATE pending_reports SET status = 'settled', receipt_json = ? WHERE ref = ? AND status = 'pending'`);
  const _markFailed  = db.prepare(`UPDATE pending_reports SET status = 'failed',  receipt_json = ? WHERE ref = ? AND status = 'pending'`);

  return {
    // Durably enqueue a call-end report. Returns true iff THIS caller won the single-winner race
    // (a fresh row was inserted); false if the ref was already queued (duplicate call-end).
    enqueue(ref, facts, nonce, now, meta = null) {
      const info = _insert.run(ref, JSON.stringify(facts), nonce, meta ? JSON.stringify(meta) : null, now);
      return info.changes > 0;
    },
    pending() { return _pending.all(); },
    get(ref) { return _get.get(ref); },
    markAttempt(ref, now) { try { _markAttempt.run(now, ref); } catch (e) { log('warn', `markAttempt ${ref}: ${e.message}`); } },
    // Transition pending→settled. Returns true iff THIS call made the transition, so the caller
    // can fire finalization EXACTLY once (inline receipt OR drainer redelivery, never twice).
    markSettled(ref, receipt) {
      const info = _markSettled.run(JSON.stringify(receipt), ref);
      return info.changes > 0;
    },
    // Transition pending→failed on a box-signed status:'failed' receipt (a TERMINAL rejection —
    // e.g. the payment went to an account the box doesn't hold). Stops the drainer republishing;
    // the row is kept for audit. Same exactly-once semantics as markSettled.
    markFailed(ref, receipt) {
      const info = _markFailed.run(JSON.stringify(receipt), ref);
      return info.changes > 0;
    },
    // Replace the stored receipt on an ALREADY-terminal row (a box COMPLETION receipt:
    // the original said 'pending', the retry later finished the payouts). Display/audit
    // only — no status transition, no re-finalize.
    updateReceipt(ref, receipt) {
      db.prepare(`UPDATE pending_reports SET receipt_json = ? WHERE ref = ?`).run(JSON.stringify(receipt), ref);
    },
  };
}

module.exports = { createSettlementQueue };
