// ─────────────────────────────────────────────────────────────────────────────
// v4call — server.js
// Decentralised paid communications via Hive blockchain + WebRTC
//
// FORKING: Only change the constants at the top of each section marked
//          ── FORK CONFIG ── to rebrand for your own node.
//          Everything else is logic — leave it alone unless you know why.
//
// Rate format: supports both [V4CALL-RATES-V1] and [V4CALL-RATES-V2]
// V2 adds: [BLOCKED], [TOKEN:SYMBOL], TEXT-SESSION, ALLOW-BLOCKED, CHAIN, SERVER, NOSTR
// ─────────────────────────────────────────────────────────────────────────────

// Load .env file if present (local dev and Docker).
// In production via systemd, env vars are set directly in the service file.
try { require('dotenv').config(); } catch(e) { /* dotenv is optional */ }

// ─────────────────────────────────────────────────────────────────────────────
// .env duplicate-key safety net (v0.16.19+)
//
// dotenv is "last-wins": writing `FEDERATION_PEERS=A` on one line and
// `FEDERATION_PEERS=B` on the next silently drops A — process.env.FEDERATION_PEERS
// only sees B. Caught in production 2026-05-28 in a three-server mesh; one peer
// of every pair was missing from the runtime config and nobody noticed because
// Nostr presence kept federated users visible in the lobby anyway. See
// FED-RECOVERY-NOTES.md Lesson 11.
//
// This scan re-reads the raw .env file at boot, looks for any key declared
// more than once, and prints a loud warning per duplicate. Behavioural impact:
// zero — dotenv has already loaded by this point. It just makes the silent
// dedup loud so the next operator catches it in seconds rather than weeks.
//
// Skips silently if there's no .env file (production via systemd, the example
// .env.example file in CI, etc.).
//
// path/fs are require()'d inline here because the main require block below
// hasn't run yet — keeping the scan at the very top of boot means the warning
// fires before any other [config] line, so an operator running
// `docker compose logs app | head -20` sees it immediately.
try {
  const _fs   = require('fs');
  const _path = require('path');
  const envPath = _path.resolve(process.cwd(), '.env');
  if (_fs.existsSync(envPath)) {
    const raw = _fs.readFileSync(envPath, 'utf8');
    const counts = Object.create(null);
    for (const line of raw.split('\n')) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith('#')) continue;
      const m = trimmed.match(/^([A-Z_][A-Z0-9_]*)\s*=/);
      if (!m) continue;
      const key = m[1];
      counts[key] = (counts[key] || 0) + 1;
    }
    for (const [key, n] of Object.entries(counts)) {
      if (n > 1) {
        const hint = (key === 'FEDERATION_PEERS' || key === 'NOSTR_RELAYS')
          ? ' Use comma-separated form: ' + key + '=value1,value2'
          : ' dotenv keeps only the LAST occurrence.';
        console.log('[config] ⚠ multiple ' + key + ' lines in .env (' + n + ').' + hint);
      }
    }
  }
} catch (e) {
  // Don't crash the server over a diagnostic. Just log and move on.
  console.log('[config] (env duplicate-key scan skipped: ' + e.message + ')');
}

const express    = require('express');
const http       = require('http');
const { Server } = require('socket.io');
const path       = require('path');
const crypto     = require('crypto');
const fs         = require('fs');
const Database   = require('better-sqlite3');
const dhive      = require('@hiveio/dhive');
const WebSocket  = require('ws');

const app    = express();
const server = http.createServer(app);
// CORS is permissive on the Socket.io server — browsers from federated peers
// connect here to join call rooms hosted on this server (see federation
// cross-server call flow below).
const io     = new Server(server, { cors: { origin: true, credentials: true } });

app.use(express.static(path.join(__dirname, 'public')));


// ─────────────────────────────────────────────────────────────────────────────
// ── FORK CONFIG — Edit .env to set these. Do NOT hardcode values here. ───────
// ─────────────────────────────────────────────────────────────────────────────

const SERVER_NAME         = process.env.SERVER_NAME         || 'v4call';
const SERVER_DOMAIN       = process.env.SERVER_DOMAIN       || 'v4call.com';
const SERVER_HIVE_ACCOUNT = process.env.SERVER_HIVE_ACCOUNT || 'v4call';
const ESCROW_ACCOUNT      = process.env.ESCROW_ACCOUNT      || 'v4call-escrow';
const PLATFORM_FEE        = parseFloat(process.env.DEFAULT_PLATFORM_FEE || '10') / 100;

// v0.16.8 — Universal precision floor for paid rates.
// Rates below this are treated as free (picker filters them out, validator
// ignores them, disbursement skips them). 0.001 matches HBD's 3-decimal
// precision and the existing `.toFixed(3)` rounding throughout the disbursement
// pipeline. For high-precision tokens (e.g. SWAP.BTC, 8 decimals) this means
// the minimum settable rate is 0.001 of the token. To support sub-millicent
// token rates (Path B in the v0.16.8 design), make this per-currency AND
// replace `.toFixed(3)` in disbursement code with per-currency precision.
const RATE_FLOOR = 0.001;
const PORT                = parseInt(process.env.PORT        || '3000');
const BIND_HOST           = process.env.BIND_HOST            || '127.0.0.1';

// Hive API nodes — can override the primary node via HIVE_API env var.
// Server tries each in order and falls back automatically on failure.
// Refreshed 2026-04: dropped anyx.io and hived.emre.sh (intermittent/dead),
// added arcange / openhive / techcoderx as known-reliable fallbacks.
const HIVE_API       = process.env.HIVE_API || 'https://api.hive.blog';
const HIVE_API_NODES = [
  HIVE_API,
  'https://api.deathwing.me',
  'https://hive-api.arcange.eu',
  'https://api.openhive.network',
  'https://techcoderx.com'
].filter((v, i, a) => a.indexOf(v) === i); // deduplicate if HIVE_API matches a fallback

// Call behaviour — tunable via .env
const CALL_COOLDOWN_MS          = parseInt(process.env.CALL_COOLDOWN_MS          || '30000');
const MAX_CALL_DURATION_MIN     = parseInt(process.env.MAX_CALL_DURATION_MIN      || '120');
const PAYMENT_VERIFY_RETRIES    = parseInt(process.env.PAYMENT_VERIFY_RETRIES     || '3');
const PAYMENT_VERIFY_DELAY_MS   = parseInt(process.env.PAYMENT_VERIFY_DELAY_MS    || '5000');

// Chat storage — tunable via .env
const DM_RETENTION_DAYS         = parseInt(process.env.DM_RETENTION_DAYS          || '33');
const ROOM_RETENTION_DAYS       = parseInt(process.env.ROOM_RETENTION_DAYS        || '33');
const DM_PREVIEW_COUNT          = parseInt(process.env.DM_PREVIEW_COUNT           || '1'); // 0 = off

// Federation — comma-separated list of peer WebSocket URLs (e.g. wss://peer.com/federation)
const FEDERATION_PEERS = (process.env.FEDERATION_PEERS || '')
  .split(',').map(s => s.trim()).filter(Boolean);
const FEDERATION_ENABLED = FEDERATION_PEERS.length > 0;
const FEDERATION_VERSION = '0.4';

// ── Nostr federation discovery (Phase B: publish own announce only) ─────────
// FED_DISCOVERY_MODE controls how servers find each other:
//   hive  = today's behaviour only (2h Hive scan), no Nostr
//   nostr = Nostr relays only
//   both  = Nostr (fast) + Hive scan (fallback)  ← default, belt-and-braces
// This knob does NOT touch the WS federation transport (DMs/calls/payments
// always ride that). It only affects discovery/presence. Phases C/D inherit it.
const FED_DISCOVERY_MODE   = (process.env.FED_DISCOVERY_MODE || 'both').toLowerCase();
const NOSTR_RELAYS         = (process.env.NOSTR_RELAYS || '')
  .split(',').map(s => s.trim()).filter(Boolean);
const NOSTR_REPUBLISH_HOURS = parseInt(process.env.NOSTR_REPUBLISH_HOURS || '6');
const NOSTR_NSEC           = process.env.NOSTR_NSEC || '';   // one-time seed only
const NOSTR_KEY_PATH       = process.env.NOSTR_KEY_PATH || '/app/nostr/nostr-key.json';
// When FED_DISCOVERY_MODE=nostr, should the 2h Hive scan still run as a
// silent safety net? Default true. Set false ONLY for deliberate Nostr-only
// testing — losing the Hive net means a total relay outage = no discovery.
const NOSTR_HIVE_FALLBACK  = (process.env.NOSTR_HIVE_FALLBACK || 'true').toLowerCase() !== 'false';
// Hive discovery scan runs UNLESS we're in pure-Nostr-no-fallback test mode.
const HIVE_SCAN_ENABLED    = !(FED_DISCOVERY_MODE === 'nostr' && !NOSTR_HIVE_FALLBACK);
// Nostr subscribe runs when discovery mode includes Nostr.
const NOSTR_SUBSCRIBE_ENABLED = (FED_DISCOVERY_MODE === 'nostr' || FED_DISCOVERY_MODE === 'both');
// ── Phase D — cross-server presence via Nostr (WS-wins-Nostr-additive) ─────
// Master gate. Default off until proven; flip to true to opt in per server.
const FED_PRESENCE_VIA_NOSTR        = (process.env.FED_PRESENCE_VIA_NOSTR || 'false').toLowerCase() === 'true';
// At most one publish per N seconds (joins/leaves coalesce inside the window).
const NOSTR_PRESENCE_THROTTLE_SECONDS  = parseInt(process.env.NOSTR_PRESENCE_THROTTLE_SECONDS  || '30');
// Republish every N seconds even if nothing changed (covers relay drops).
const NOSTR_PRESENCE_HEARTBEAT_SECONDS = parseInt(process.env.NOSTR_PRESENCE_HEARTBEAT_SECONDS || '60');
// Drop a peer's Nostr presence if we haven't heard from it for N seconds.
const NOSTR_PRESENCE_TTL_SECONDS       = parseInt(process.env.NOSTR_PRESENCE_TTL_SECONDS       || '300');

console.log(`[config] Server:       ${SERVER_NAME} (${SERVER_DOMAIN})`);
console.log(`[config] Escrow:       @${ESCROW_ACCOUNT}`);
console.log(`[config] Platform fee: ${PLATFORM_FEE * 100}%`);
console.log(`[config] Max duration: ${MAX_CALL_DURATION_MIN} min`);
console.log(`[config] DM retention: ${DM_RETENTION_DAYS} days | Room retention: ${ROOM_RETENTION_DAYS} days | DM preview: ${DM_PREVIEW_COUNT}`);
if (FEDERATION_ENABLED) {
  console.log(`[config] Federation: ENABLED — peers: ${FEDERATION_PEERS.join(', ')}`);
} else {
  console.log(`[config] Federation: disabled (no FEDERATION_PEERS set)`);
}

// ── v0.13 Lobby Notice + Anti-Spam Gate (env only; no federation bump) ──────
const LOBBY_NOTICE_RAW = process.env.LOBBY_NOTICE || '';
const LOBBY_REQUIREMENTS_RAW = process.env.LOBBY_REQUIREMENTS_TEXT || '';
const LOBBY_POST_MIN_HP   = parseFloat(process.env.LOBBY_POST_MIN_HP   || '0') || 0;
const LOBBY_POST_MIN_HIVE = parseFloat(process.env.LOBBY_POST_MIN_HIVE || '0') || 0;
const LOBBY_POST_MIN_TOKEN_RAW = (process.env.LOBBY_POST_MIN_TOKEN || '').trim();
const LOBBY_POST_GATE_MODE = (process.env.LOBBY_POST_GATE_MODE || 'or').toLowerCase() === 'and' ? 'and' : 'or';
let LOBBY_POST_MIN_TOKEN_SYMBOL = null;
let LOBBY_POST_MIN_TOKEN_AMOUNT = 0;
if (LOBBY_POST_MIN_TOKEN_RAW.includes(':')) {
  const [sym, amt] = LOBBY_POST_MIN_TOKEN_RAW.split(':');
  LOBBY_POST_MIN_TOKEN_SYMBOL = sym.trim().toUpperCase();
  LOBBY_POST_MIN_TOKEN_AMOUNT = parseFloat(amt) || 0;
}
const LOBBY_NOTICE_RESOLVED = LOBBY_NOTICE_RAW ||
  `${SERVER_DOMAIN} — local lobby. For federated contacts use rooms / DMs / calls.`;
const LOBBY_REQUIREMENTS_RESOLVED = LOBBY_REQUIREMENTS_RAW || (() => {
  const parts = [];
  if (LOBBY_POST_MIN_HP   > 0)         parts.push(`${LOBBY_POST_MIN_HP} HP`);
  if (LOBBY_POST_MIN_HIVE > 0)         parts.push(`${LOBBY_POST_MIN_HIVE} HIVE`);
  if (LOBBY_POST_MIN_TOKEN_SYMBOL)     parts.push(`${LOBBY_POST_MIN_TOKEN_AMOUNT} ${LOBBY_POST_MIN_TOKEN_SYMBOL}`);
  if (parts.length === 0) return '';
  if (parts.length === 1) return `Posting requires ${parts[0]}.`;
  return `Posting requires ${parts.join(LOBBY_POST_GATE_MODE === 'and' ? ' AND ' : ' OR ')}.`;
})();


// ─────────────────────────────────────────────────────────────────────────────
// ── SQLite Ledger ─────────────────────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────

const LOG_DIR = path.join(__dirname, 'logs');
if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true });

const db = new Database(path.join(LOG_DIR, 'v4call-ledger.db'));

db.exec(`
  CREATE TABLE IF NOT EXISTS payments (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT    NOT NULL DEFAULT (datetime('now')),
    call_id     TEXT    NOT NULL,
    type        TEXT    NOT NULL,  -- 'ring' | 'connect' | 'payout' | 'refund' | 'platform_fee'
    from_user   TEXT    NOT NULL,
    to_user     TEXT    NOT NULL,
    amount      REAL    NOT NULL,
    currency    TEXT    NOT NULL DEFAULT 'HBD',
    memo        TEXT,
    tx_id       TEXT,              -- Hive transaction ID if available
    status      TEXT    NOT NULL DEFAULT 'pending',  -- 'pending' | 'verified' | 'sent' | 'failed'
    note        TEXT
  );

  CREATE TABLE IF NOT EXISTS calls (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    call_id         TEXT    UNIQUE NOT NULL,
    caller          TEXT    NOT NULL,
    callee          TEXT    NOT NULL,
    call_type       TEXT    NOT NULL DEFAULT 'voice',
    chain           TEXT    NOT NULL DEFAULT 'hive',
    started_at      TEXT,
    connected_at    TEXT,
    ended_at        TEXT,
    duration_min    REAL    DEFAULT 0,
    ring_paid       REAL    DEFAULT 0,
    connect_paid    REAL    DEFAULT 0,
    duration_cost   REAL    DEFAULT 0,
    callee_net      REAL    DEFAULT 0,
    platform_cut    REAL    DEFAULT 0,
    status          TEXT    NOT NULL DEFAULT 'initiated',
    end_reason      TEXT
  );

  CREATE INDEX IF NOT EXISTS idx_calls_caller   ON calls(caller);
  CREATE INDEX IF NOT EXISTS idx_calls_callee   ON calls(callee);
  CREATE INDEX IF NOT EXISTS idx_calls_call_id  ON calls(call_id);
  CREATE INDEX IF NOT EXISTS idx_payments_call_id ON payments(call_id);
`);

console.log('[ledger] SQLite ready:', path.join(LOG_DIR, 'v4call-ledger.db'));


// ─────────────────────────────────────────────────────────────────────────────
// ── SQLite Chat Store (separate DB for security) ─────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────

const chatDb = new Database(path.join(LOG_DIR, 'v4call-chat.db'));

chatDb.exec(`
  CREATE TABLE IF NOT EXISTS dm_messages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT    NOT NULL DEFAULT (datetime('now')),
    from_user   TEXT    NOT NULL,
    to_user     TEXT    NOT NULL,
    owner       TEXT    NOT NULL,
    ciphertext  TEXT    NOT NULL,
    signature   TEXT,
    timestamp   TEXT,
    text_paid   REAL    DEFAULT 0,
    created_at  TEXT    NOT NULL DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS room_messages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT    NOT NULL DEFAULT (datetime('now')),
    room_name   TEXT    NOT NULL,
    from_user   TEXT    NOT NULL,
    to_user     TEXT    NOT NULL,
    ciphertext  TEXT    NOT NULL,
    signature   TEXT,
    timestamp   TEXT,
    is_broadcast INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT    NOT NULL DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS user_seen (
    username    TEXT    PRIMARY KEY,
    last_seen   TEXT    NOT NULL DEFAULT (datetime('now'))
  );

  -- ipfs-gate v0.1 attachment envelopes. One row per emit. File bytes live on
  -- ipfs-gate (not here); we store only the envelope so room-history can replay
  -- attachment bubbles on rejoin. per_recipient is a JSON map { username: encKey }.
  -- Kept even past expires_at so users see "someone sent X (now gone)" with a
  -- ⚠ 404 chip when the client tries to fetch the expired pin.
  CREATE TABLE IF NOT EXISTS room_attachments (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    room_name         TEXT    NOT NULL,
    sender            TEXT    NOT NULL,
    sender_pubkey     TEXT    NOT NULL,
    cid               TEXT    NOT NULL,
    size_bytes        INTEGER NOT NULL,
    envelope_sig      TEXT    NOT NULL,
    env_created_at    TEXT    NOT NULL,
    expires_at        TEXT,
    gateway_hint      TEXT,
    kind_hint         TEXT,
    per_recipient     TEXT    NOT NULL,
    original_filename TEXT,
    original_mime     TEXT,
    original_size     INTEGER,
    stored_at         TEXT    NOT NULL DEFAULT (datetime('now'))
  );

  -- DM attachments (v0.16.17). Same envelope shape as room_attachments but
  -- the context is a 1:1 conversation (sender ↔ to_user) rather than a room.
  -- per_recipient is always exactly { sender: encKey, to_user: encKey }.
  -- text_paid + currency mirror dm_messages so the paid-DM trail is
  -- auditable per-attachment. Local-server only in v0.1 (no federation).
  CREATE TABLE IF NOT EXISTS dm_attachments (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    sender            TEXT    NOT NULL,
    to_user           TEXT    NOT NULL,
    sender_pubkey     TEXT    NOT NULL,
    cid               TEXT    NOT NULL,
    size_bytes        INTEGER NOT NULL,
    envelope_sig      TEXT    NOT NULL,
    env_created_at    TEXT    NOT NULL,
    expires_at        TEXT,
    gateway_hint      TEXT,
    kind_hint         TEXT,
    per_recipient     TEXT    NOT NULL,
    original_filename TEXT,
    original_mime     TEXT,
    original_size     INTEGER,
    text_paid         REAL    NOT NULL DEFAULT 0,
    currency          TEXT    NOT NULL DEFAULT 'HBD',
    stored_at         TEXT    NOT NULL DEFAULT (datetime('now'))
  );

  CREATE INDEX IF NOT EXISTS idx_dm_owner      ON dm_messages(owner);
  CREATE INDEX IF NOT EXISTS idx_dm_from       ON dm_messages(from_user);
  CREATE INDEX IF NOT EXISTS idx_dm_to         ON dm_messages(to_user);
  CREATE INDEX IF NOT EXISTS idx_dm_created     ON dm_messages(created_at);
  CREATE INDEX IF NOT EXISTS idx_room_name     ON room_messages(room_name);
  CREATE INDEX IF NOT EXISTS idx_room_created  ON room_messages(created_at);
  CREATE INDEX IF NOT EXISTS idx_room_att_room    ON room_attachments(room_name);
  CREATE INDEX IF NOT EXISTS idx_room_att_stored  ON room_attachments(stored_at);
  CREATE INDEX IF NOT EXISTS idx_dm_att_sender   ON dm_attachments(sender);
  CREATE INDEX IF NOT EXISTS idx_dm_att_to       ON dm_attachments(to_user);
  CREATE INDEX IF NOT EXISTS idx_dm_att_stored   ON dm_attachments(stored_at);
`);

// Migration: paid-DM badge needs the actual currency. Older rows back-default
// to 'HBD' which is wrong for token DMs but unfixable without payment metadata.
try { chatDb.exec(`ALTER TABLE dm_messages ADD COLUMN currency TEXT DEFAULT 'HBD'`); }
catch(e) { /* duplicate column = already migrated */ }

console.log('[chat] SQLite ready:', path.join(LOG_DIR, 'v4call-chat.db'));

// ── Chat DB helpers ──────────────────────────────────────────────────────────

function chatStoreDm(fromUser, toUser, ciphertextForRecipient, ciphertextForSender, signature, timestamp, textPaid, currency) {
  try {
    const cur = currency || 'HBD';
    const stmt = chatDb.prepare(`
      INSERT INTO dm_messages (from_user, to_user, owner, ciphertext, signature, timestamp, text_paid, currency)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);
    // Store recipient's copy (encrypted to recipient's key)
    stmt.run(fromUser, toUser, toUser, ciphertextForRecipient, signature, timestamp, textPaid || 0, cur);
    // Store sender's copy (encrypted to sender's key)
    if (ciphertextForSender) {
      stmt.run(fromUser, toUser, fromUser, ciphertextForSender, signature, timestamp, textPaid || 0, cur);
    }
  } catch(e) {
    console.error('[chat] DM store failed:', e.message);
  }
}

function chatStoreRoomMsg(roomName, fromUser, toUser, ciphertext, signature, timestamp, isBroadcast) {
  try {
    chatDb.prepare(`
      INSERT INTO room_messages (room_name, from_user, to_user, ciphertext, signature, timestamp, is_broadcast)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(roomName, fromUser, toUser, ciphertext, signature, timestamp, isBroadcast ? 1 : 0);
  } catch(e) {
    console.error('[chat] Room message store failed:', e.message);
  }
}

function chatGetDmUnread(username) {
  try {
    const seen = chatDb.prepare(`SELECT last_seen FROM user_seen WHERE username = ?`).get(username);
    const since = seen ? seen.last_seen : '1970-01-01 00:00:00';
    const rows = chatDb.prepare(`
      SELECT from_user, COUNT(*) as cnt
      FROM dm_messages
      WHERE owner = ? AND to_user = ? AND created_at > ?
      GROUP BY from_user
    `).all(username, username, since);
    return rows; // [{ from_user, cnt }, ...]
  } catch(e) {
    console.error('[chat] Unread count failed:', e.message);
    return [];
  }
}

function chatGetDmPreviews(username, countPerUser) {
  if (countPerUser <= 0) return [];
  try {
    // Get the N most recent messages per conversation partner
    const partners = chatDb.prepare(`
      SELECT DISTINCT CASE WHEN from_user = ? THEN to_user ELSE from_user END as partner
      FROM dm_messages WHERE owner = ?
    `).all(username, username);

    const previews = [];
    const stmt = chatDb.prepare(`
      SELECT from_user, to_user, ciphertext, signature, timestamp, text_paid, currency, created_at
      FROM dm_messages
      WHERE owner = ? AND (
        (from_user = ? AND to_user = ?) OR (from_user = ? AND to_user = ?)
      )
      ORDER BY created_at DESC LIMIT ?
    `);
    for (const { partner } of partners) {
      const rows = stmt.all(username, username, partner, partner, username, countPerUser);
      previews.push(...rows.reverse()); // chronological order
    }
    return previews;
  } catch(e) {
    console.error('[chat] DM preview failed:', e.message);
    return [];
  }
}

function chatGetDmHistory(username, withUser) {
  try {
    return chatDb.prepare(`
      SELECT from_user, to_user, ciphertext, signature, timestamp, text_paid, currency, created_at
      FROM dm_messages
      WHERE owner = ? AND (
        (from_user = ? AND to_user = ?) OR (from_user = ? AND to_user = ?)
      )
      ORDER BY created_at ASC
    `).all(username, username, withUser, withUser, username);
  } catch(e) {
    console.error('[chat] DM history failed:', e.message);
    return [];
  }
}

function chatGetRoomHistory(roomName, username) {
  try {
    return chatDb.prepare(`
      SELECT from_user, to_user, ciphertext, signature, timestamp, is_broadcast, created_at
      FROM room_messages
      WHERE room_name = ? AND (is_broadcast = 1 OR to_user = ? OR from_user = ?)
      ORDER BY created_at ASC
    `).all(roomName, username, username);
  } catch(e) {
    console.error('[chat] Room history failed:', e.message);
    return [];
  }
}

function chatDeleteRoom(roomName) {
  try {
    const info = chatDb.prepare(`DELETE FROM room_messages WHERE room_name = ?`).run(roomName);
    if (info.changes > 0) console.log(`[chat] Deleted ${info.changes} messages for room #${roomName}`);
    const aInfo = chatDb.prepare(`DELETE FROM room_attachments WHERE room_name = ?`).run(roomName);
    if (aInfo.changes > 0) console.log(`[chat] Deleted ${aInfo.changes} attachment envelopes for room #${roomName}`);
  } catch(e) {
    console.error('[chat] Room delete failed:', e.message);
  }
}

function chatStoreRoomAttachment(env) {
  try {
    chatDb.prepare(`
      INSERT INTO room_attachments (
        room_name, sender, sender_pubkey, cid, size_bytes, envelope_sig,
        env_created_at, expires_at, gateway_hint, kind_hint, per_recipient,
        original_filename, original_mime, original_size
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      env.room, env.sender, env.sender_pubkey || '', env.cid,
      env.size_bytes | 0, env.envelope_sig, env.created_at || new Date().toISOString(),
      env.expires_at || null, env.gateway_hint || null, env.kind_hint || null,
      JSON.stringify(env.per_recipient || {}),
      env.original_filename || null, env.original_mime || null,
      env.original_size != null ? (env.original_size | 0) : null
    );
  } catch (e) {
    console.error('[chat] Room attachment store failed:', e.message);
  }
}

// History query: returns full envelopes for the room where the requesting user
// is either the sender OR present in per_recipient. Bystander-by-default for
// late joiners (they don't see attachments addressed to others sent before they
// joined — same privacy posture as chatGetRoomHistory for encrypted DMs).
function chatGetRoomAttachments(roomName, username) {
  try {
    const rows = chatDb.prepare(`
      SELECT * FROM room_attachments
      WHERE room_name = ? ORDER BY stored_at ASC
    `).all(roomName);
    const out = [];
    for (const r of rows) {
      let per_recipient = {};
      try { per_recipient = JSON.parse(r.per_recipient || '{}'); } catch (_) {}
      if (r.sender !== username && !(username in per_recipient)) continue;
      out.push({
        v: 1, type: 'room-attachment',
        room: r.room_name, sender: r.sender, sender_pubkey: r.sender_pubkey,
        cid: r.cid, size_bytes: r.size_bytes, envelope_sig: r.envelope_sig,
        created_at: r.env_created_at, expires_at: r.expires_at,
        gateway_hint: r.gateway_hint, kind_hint: r.kind_hint,
        per_recipient,
        original_filename: r.original_filename,
        original_mime: r.original_mime,
        original_size: r.original_size
      });
    }
    return out;
  } catch (e) {
    console.error('[chat] Room attachment history failed:', e.message);
    return [];
  }
}

// v0.16.17 — DM attachment persistence + history. Mirrors the room helpers
// but keyed on the (sender, to_user) conversation pair. text_paid + currency
// captured so the paid-DM trail per attachment is auditable.
function chatStoreDmAttachment(env, textPaid, currency) {
  try {
    chatDb.prepare(`
      INSERT INTO dm_attachments (
        sender, to_user, sender_pubkey, cid, size_bytes, envelope_sig,
        env_created_at, expires_at, gateway_hint, kind_hint, per_recipient,
        original_filename, original_mime, original_size, text_paid, currency
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      env.sender, env.to_user, env.sender_pubkey || '', env.cid,
      env.size_bytes | 0, env.envelope_sig, env.created_at || new Date().toISOString(),
      env.expires_at || null, env.gateway_hint || null, env.kind_hint || null,
      JSON.stringify(env.per_recipient || {}),
      env.original_filename || null, env.original_mime || null,
      env.original_size != null ? (env.original_size | 0) : null,
      textPaid || 0, currency || 'HBD'
    );
  } catch (e) {
    console.error('[chat] DM attachment store failed:', e.message);
  }
}

// History query for the (username, withUser) conversation. Returns envelopes
// for either direction (sender = username AND to_user = withUser, OR sender =
// withUser AND to_user = username) where the requesting user is in
// per_recipient. Envelopes kept past expires_at — client renders ⚠ 404.
function chatGetDmAttachments(username, withUser) {
  try {
    const rows = chatDb.prepare(`
      SELECT * FROM dm_attachments
      WHERE (sender = ? AND to_user = ?) OR (sender = ? AND to_user = ?)
      ORDER BY stored_at ASC
    `).all(username, withUser, withUser, username);
    const out = [];
    for (const r of rows) {
      let per_recipient = {};
      try { per_recipient = JSON.parse(r.per_recipient || '{}'); } catch (_) {}
      // Defensive — for DMs the audience is always {sender, to_user} so this
      // should always pass; included for symmetry with the room query.
      if (r.sender !== username && !(username in per_recipient)) continue;
      out.push({
        v: 1, type: 'dm-attachment',
        to_user: r.to_user, sender: r.sender, sender_pubkey: r.sender_pubkey,
        cid: r.cid, size_bytes: r.size_bytes, envelope_sig: r.envelope_sig,
        created_at: r.env_created_at, expires_at: r.expires_at,
        gateway_hint: r.gateway_hint, kind_hint: r.kind_hint,
        per_recipient,
        original_filename: r.original_filename,
        original_mime: r.original_mime,
        original_size: r.original_size,
        text_paid: r.text_paid, currency: r.currency
      });
    }
    return out;
  } catch (e) {
    console.error('[chat] DM attachment history failed:', e.message);
    return [];
  }
}

// v0.14.5 — every row for a room, regardless of recipient. Used by the
// .v4room export endpoint; per-recipient filtering happens client-side at
// decryption time (since the ciphertext is what's stored).
function chatGetRoomMessagesAll(roomName) {
  try {
    return chatDb.prepare(`
      SELECT from_user, to_user, ciphertext, signature, timestamp, is_broadcast, created_at
      FROM room_messages WHERE room_name = ? ORDER BY created_at ASC
    `).all(roomName);
  } catch(e) {
    console.error('[chat] Room export query failed:', e.message);
    return [];
  }
}

function chatUpdateSeen(username) {
  try {
    chatDb.prepare(`
      INSERT INTO user_seen (username, last_seen) VALUES (?, datetime('now'))
      ON CONFLICT(username) DO UPDATE SET last_seen = datetime('now')
    `).run(username);
  } catch(e) {
    console.error('[chat] Seen update failed:', e.message);
  }
}

function chatCleanup() {
  try {
    const dmCutoff   = chatDb.prepare(`SELECT datetime('now', ?) as cutoff`).get(`-${DM_RETENTION_DAYS} days`).cutoff;
    const roomCutoff = chatDb.prepare(`SELECT datetime('now', ?) as cutoff`).get(`-${ROOM_RETENTION_DAYS} days`).cutoff;
    const dmDel    = chatDb.prepare(`DELETE FROM dm_messages WHERE created_at < ?`).run(dmCutoff);
    const roomDel  = chatDb.prepare(`DELETE FROM room_messages WHERE created_at < ?`).run(roomCutoff);
    const dmAttDel = chatDb.prepare(`DELETE FROM dm_attachments WHERE stored_at < ?`).run(dmCutoff);
    if (dmDel.changes || roomDel.changes || dmAttDel.changes) {
      console.log(`[chat] Cleanup: removed ${dmDel.changes} DMs, ${roomDel.changes} room messages, ${dmAttDel.changes} DM attachments`);
    }
  } catch(e) {
    console.error('[chat] Cleanup failed:', e.message);
  }
}

// Run cleanup on startup and then every hour
chatCleanup();
setInterval(chatCleanup, 60 * 60 * 1000);

// ── Ledger helpers ────────────────────────────────────────────────────────────

function ledgerPayment(callId, type, fromUser, toUser, amount, memo = '', status = 'pending', txId = null, currency = 'HBD') {
  try {
    db.prepare(`
      INSERT INTO payments (call_id, type, from_user, to_user, amount, currency, memo, status, tx_id)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(callId, type, fromUser, toUser, amount, currency, memo, status, txId);
  } catch(e) {
    console.error('[ledger] Payment insert failed:', e.message);
  }
}

// v0.16.9 — Refundable missed/declined/cancelled calls for the callee.
// Returns rows where the caller paid a ring fee that's still in our escrow
// (no refund payment row exists yet). Used by the missed-call popup on login.
function getRefundableMissedCalls(callee, limit = 20) {
  try {
    return db.prepare(`
      SELECT
        c.call_id,
        c.caller,
        c.callee,
        c.call_type,
        c.started_at,
        c.status,
        c.end_reason,
        p.amount   AS ring_paid,
        p.currency AS ring_currency,
        p.memo     AS ring_memo
      FROM calls c
      JOIN payments p
        ON p.call_id = c.call_id
       AND p.type    = 'ring'
       AND p.status IN ('verified', 'sent')
      WHERE c.callee = ?
        AND c.status IN ('missed', 'declined', 'cancelled')
        AND p.amount > 0
        AND NOT EXISTS (
          SELECT 1 FROM payments r
          WHERE r.call_id = c.call_id
            AND r.type IN ('refund', 'ring_refund')
            AND r.status IN ('pending', 'sent')
        )
      ORDER BY c.started_at DESC
      LIMIT ?
    `).all(callee, limit);
  } catch(e) {
    console.error('[ledger] getRefundableMissedCalls failed:', e.message);
    return [];
  }
}

function ledgerCallCreate(callId, caller, callee, callType = 'voice') {
  try {
    db.prepare(`
      INSERT OR IGNORE INTO calls (call_id, caller, callee, call_type, started_at, status)
      VALUES (?, ?, ?, ?, datetime('now'), 'initiated')
    `).run(callId, caller, callee, callType);
  } catch(e) {
    console.error('[ledger] Call create failed:', e.message);
  }
}

function ledgerCallUpdate(callId, fields) {
  const keys = Object.keys(fields);
  if (!keys.length) return;
  const set  = keys.map(k => k + ' = ?').join(', ');
  const vals = [...keys.map(k => fields[k]), callId];
  try {
    db.prepare(`UPDATE calls SET ${set} WHERE call_id = ?`).run(...vals);
  } catch(e) {
    console.error('[ledger] Call update failed:', e.message);
  }
}

function ledgerPaymentUpdate(callId, type, status, txId = null) {
  try {
    db.prepare(`
      UPDATE payments SET status = ?, tx_id = COALESCE(?, tx_id)
      WHERE call_id = ? AND type = ?
    `).run(status, txId, callId, type);
  } catch(e) {
    console.error('[ledger] Payment update failed:', e.message);
  }
}


// ─────────────────────────────────────────────────────────────────────────────
// ── Lobby & Rooms ─────────────────────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────

const lobbyUsers = {}; // username → { socketId, pubKey, invisible, inCall? }
// v0.16.17 — debounce offline broadcasts for ≤5s to soak transient socket
// drops (laptop sleep wake, wifi blip). Keyed by username; cleared on
// lobby-join. See the disconnect handler for the firing semantics.
const pendingOfflineTimers = {};
const rooms      = {}; // roomName → { creator, allowlist(Set), banlist(Set), tokenGate:{symbol,amount}|null, banlistVisibility:'admin'|'all', paidInvitees:Map, members[], createdAt, isCall?, callId?, ... }

// Federation state — populated by federation message handlers (see below).
// domain → { ws, connected, name, users: Map(username → { pubKey }), protocolVersion }
const federationPeers = {};

// In-flight federated room invites (v0.16, fed v0.4). One map serves both
// directions; `dir` distinguishes:
//   outgoing — admin on this server invited a user on another server
//   incoming — peer server invited a local user to a room on the peer
// Pruned by ttl-sweep (see start of fedHandleMessage section).
const pendingFederatedInvites = {};
const FED_INVITE_TTL_MS = 15 * 60 * 1000;

// v0.16.10 — paid LOCAL room invites awaiting recipient accept/decline. When
// an admin pays an invitee's room-invite fee, the funds sit in escrow under
// this key; on decline / 15-min timeout / explicit cancel the inviter is
// refunded the gross-paid amount. On accept, the recipient is paid net of
// platform fee. Keyed by inviteId (random hex).
//   { room, inviter, invitee, currency, paid, memo, msgId, status,
//     created_at, type: 'create' | 'allowlist' }
// status: 'pending' (sent, awaiting response) | 'accepted' | 'declined' |
//         'timed_out' | 'cancelled'
const pendingPaidInvites = {};
const PAID_INVITE_TTL_MS = 15 * 60 * 1000;

// ── Discovery state (populated by scanV4CallDirectory) ──────────────────────
// domain → { post_author, post_permlink, post_created, parsed, verified,
//            verify_reason, last_seen }
const discoveredPeers = {};

// Domains the operator has explicitly approved to federate. Approval is
// required for both inbound acceptance and outbound initiation. Seeded from
// FEDERATION_PEERS env (auto-approved, backwards compat) + loaded from
// /app/logs/approved-peers.json on startup. Persisted on mutation.
const approvedPeers = new Set();

function _approvedPeersFile() { return path.join(LOG_DIR, 'approved-peers.json'); }

function loadApprovedPeers() {
  // 1. Seed from FEDERATION_PEERS env — operator-declared trust is implicit.
  for (const url of FEDERATION_PEERS) {
    try { approvedPeers.add(new URL(url).host.toLowerCase()); }
    catch(e) { /* bad URL, ignore */ }
  }
  // 2. Load persisted approvals from previous runs.
  try {
    const raw  = fs.readFileSync(_approvedPeersFile(), 'utf8');
    const list = JSON.parse(raw);
    if (Array.isArray(list)) for (const d of list) if (typeof d === 'string') approvedPeers.add(d.toLowerCase());
  } catch(e) { /* file doesn't exist yet — first run, fine */ }
}

function persistApprovedPeers() {
  try {
    fs.writeFileSync(_approvedPeersFile(), JSON.stringify([...approvedPeers].sort(), null, 2));
  } catch(e) { console.error('[peers] Failed to persist approval list:', e.message); }
}

function federatedUserSnapshot() {
  const out = [];
  for (const [domain, peer] of Object.entries(federationPeers)) {
    if (!peer.connected) continue;
    for (const [username, u] of peer.users) {
      // Skip collisions with local users — local identity wins to avoid spoofing.
      if (lobbyUsers[username]) continue;
      out.push({ username, pubKey: u.pubKey, server: domain });
    }
  }
  return out;
}

function lobbySnapshot() {
  const local = Object.entries(lobbyUsers)
    .filter(([, u]) => !u.invisible)
    .map(([username, u]) => ({ username, socketId: u.socketId, pubKey: u.pubKey, server: SERVER_DOMAIN }));
  return [...local, ...federatedUserSnapshot(), ...nostrAdditivePresenceSnapshot()];
}
function broadcastLobby() {
  io.emit('lobby-users', lobbySnapshot());
  // Phase D: local presence may have changed → tell nostr-fed to publish
  // (throttled + heartbeat-protected inside the module). No-op if Phase D
  // is gated off or the module hasn't loaded yet.
  try { nostrFedController?.notePresenceChange(); } catch { /* never crash on a Nostr hiccup */ }
}

// ── Phase D — cross-server presence via Nostr (WS-wins-Nostr-additive) ─────
// Map of domain → { users:Set<string>, lastTs:number(unix s), lastEventId,
// pubkey, updatedAt:ms }. Only domains we've Phase-C-discovered + verified
// land here, and only events from the expected (Hive-anchored when possible)
// pubkey are accepted. Stale entries time out via the TTL sweep.
const nostrCrossFedPresence = {};
let nostrFedController = null;  // populated after dynamic import; see startup block

// Trust gate — what pubkey is THIS domain allowed to sign presence events with?
// Prefer the Hive-signature-anchored binding from verifyPeer's Option B
// canonical (`verified_nostr_hex`). Fall back to the Phase C "poke" binding
// (`nostr_pubkey` recorded when a Nostr discovery event arrived). If neither
// exists, we have no acceptable binding → reject.
function expectedNostrHexForDomain(domain) {
  const p = discoveredPeers[domain];
  if (!p || !p.verified) return null;
  return (p.verified_nostr_hex || p.nostr_pubkey || null);
}

// Called when a verified-shape v4call-presence event arrives. Trust gate is
// already strict in the module (own-pubkey skip, content/d-tag domain match);
// here we make the FINAL trust decision against the Hive-anchored binding.
function recordNostrPresence({ domain, users, pubkey, eventId, ts }) {
  try {
    if (!FED_PRESENCE_VIA_NOSTR) return;        // master gate
    if (!domain || !pubkey) return;
    domain = String(domain).toLowerCase();
    if (domain === SERVER_DOMAIN.toLowerCase()) return;   // can't be us

    const expected = expectedNostrHexForDomain(domain);
    if (!expected) {
      // Peer not Phase-C-verified yet (or no Nostr binding for it). Drop.
      // We do NOT silently buffer — they'll publish again at heartbeat.
      return;
    }
    if (expected !== pubkey) {
      console.warn(`[presence] ✗ pubkey mismatch from relay for @${domain}: expected ${expected.slice(0,12)}…, got ${pubkey.slice(0,12)}… — dropped`);
      return;
    }

    // Newer-wins per domain (relays can echo older events).
    const cur = nostrCrossFedPresence[domain];
    if (cur && ts <= cur.lastTs) return;

    const userSet = new Set(
      (Array.isArray(users) ? users : [])
        .map(u => String(u || '').trim().toLowerCase())
        .filter(Boolean)
    );
    nostrCrossFedPresence[domain] = {
      users:     userSet,
      lastTs:    ts,
      lastEventId: eventId,
      pubkey,
      updatedAt: Date.now(),
    };
    // Tell every connected client right away — the user list now has more
    // people in it. broadcastLobby will call back into nostr-fed to publish
    // our local change too; that's fine and throttle-protected.
    io.emit('lobby-users', lobbySnapshot());
  } catch (e) {
    console.error('[presence] recordNostrPresence error (non-fatal):', e.message);
  }
}

// Reconciliation — WS wins, Nostr is purely additive (never marks offline).
// For each domain we have Nostr presence for, return ONLY the users that the
// WS federation hasn't already reported for that same domain. Result: if WS
// is healthy, this is empty. If WS is lagging or down, Nostr fills the gap.
//
// v0.16.15 — visibility tracks approval. ONLY surface presence from domains
// the operator has approved. A verified-but-unapproved peer can sit in
// discoveredPeers (and on /admin-peers.html waiting for review) without
// polluting the lobby. This closes the social-engineering / lobby-spam
// surface a bad actor with a real Hive account could otherwise have used.
// The recordNostrPresence write side STAYS unchanged so the moment you
// approve a previously-rejected peer, their users appear on the next
// broadcastLobby (no need to wait for a heartbeat).
function nostrAdditivePresenceSnapshot() {
  if (!FED_PRESENCE_VIA_NOSTR) return [];
  // What WS-federation users do we already have, grouped by domain?
  const wsByDomain = {};
  for (const u of federatedUserSnapshot()) {
    const d = String(u.server || '').toLowerCase();
    if (!d) continue;
    if (!wsByDomain[d]) wsByDomain[d] = new Set();
    wsByDomain[d].add(String(u.username || '').toLowerCase());
  }
  const out = [];
  for (const [domain, entry] of Object.entries(nostrCrossFedPresence)) {
    // v0.16.15 — approval is the single switch for cross-server visibility.
    if (!approvedPeers.has(domain)) continue;
    const wsSet = wsByDomain[domain] || new Set();
    for (const username of entry.users) {
      if (wsSet.has(username)) continue;        // WS already reports them
      // Cached pubKey for DM encryption (may be present from a prior call/DM).
      const cachedPub = pubKeyCache && pubKeyCache[username];
      out.push({
        username,
        socketId: null,                          // no live socket here yet
        pubKey:   cachedPub || null,
        server:   domain,
        source:   'nostr',                       // hint; client treats same as fed
      });
    }
  }
  return out;
}

// Periodic sweep — drop any domain whose Nostr presence is older than the TTL.
// Runs even with Phase D off (cheap empty-map walk) so flipping the gate at
// runtime via `.env` + restart is clean.
function sweepStaleNostrPresence() {
  if (!FED_PRESENCE_VIA_NOSTR) return;
  const cutoff = Date.now() - NOSTR_PRESENCE_TTL_SECONDS * 1000;
  let dropped = 0;
  for (const [domain, entry] of Object.entries(nostrCrossFedPresence)) {
    if (entry.updatedAt < cutoff) {
      delete nostrCrossFedPresence[domain];
      dropped++;
      console.log(`[presence] ⌛ dropped stale Nostr presence for @${domain} (no update in ${NOSTR_PRESENCE_TTL_SECONDS}s)`);
    }
  }
  if (dropped) io.emit('lobby-users', lobbySnapshot());
}
setInterval(sweepStaleNostrPresence, 60 * 1000).unref();

// Helper for nostr-fed.mjs's presence publish: snapshot of our LOCAL online
// usernames only (NOT federated — peers will report their own). Sorted so
// re-publishes produce stable bytes when nothing changed.
function getLocalOnlineUsernamesForPresence() {
  return Object.entries(lobbyUsers)
    .filter(([, u]) => !u.invisible)
    .map(([username]) => username.toLowerCase())
    .sort();
}

// Cached pubKeys so Nostr-only-additive users can still be DMed if we've
// ever seen their pubKey before (e.g. a prior call/DM). Populated by the
// existing user-event handlers below.
const pubKeyCache = {};

// Look up which federation peer hosts a given username. Returns the peer
// record (with ws) or null if no federated peer has that user.
function peerForUser(username) {
  for (const [domain, peer] of Object.entries(federationPeers)) {
    if (!peer.connected) continue;
    if (peer.users.has(username)) return { domain, ...peer };
  }
  return null;
}

// v0.16.18 — Is this username visible via Nostr-additive presence (Phase D)
// from a domain whose WS federation isn't currently connected? Used to give
// a "WS reconnecting, try again" error instead of "not online" when the lobby
// shows the user but routing isn't possible yet.
// Returns the domain string or null.
function nostrSeenDomain(username) {
  if (!FED_PRESENCE_VIA_NOSTR) return null;
  const u = String(username || '').toLowerCase();
  if (!u) return null;
  for (const [domain, entry] of Object.entries(nostrCrossFedPresence)) {
    if (!approvedPeers.has(domain)) continue;
    if (entry.users && entry.users.has(u)) return domain;
  }
  return null;
}

// Combined routing resolver — single source of truth that mirrors what the
// lobby snapshot shows. Callers should use this instead of peerForUser when
// they need to distinguish "user truly offline" from "user visible via Nostr
// but WS federation isn't connected yet" (a transient state after restart).
//
// Return shapes:
//   { status: 'local', recipient }                  — local socket, ready
//   { status: 'federated', peer }                   — WS peer connected
//   { status: 'nostr-only', domain }                — visible via Nostr, WS down
//   { status: 'offline' }                           — not visible anywhere
function recipientStatus(username) {
  const recipient = lobbyUsers[username];
  if (recipient) return { status: 'local', recipient };
  if (FEDERATION_ENABLED) {
    const peer = peerForUser(username);
    if (peer) return { status: 'federated', peer };
  }
  const domain = nostrSeenDomain(username);
  if (domain) return { status: 'nostr-only', domain };
  return { status: 'offline' };
}

function roomsSnapshot() {
  return Object.entries(rooms).map(([name, r]) => ({
    name,
    creator:     r.creator,
    memberCount: r.members.length,
    isCall:      r.isCall || false,
    tokenGate:   r.tokenGate || null,
    allowlist:   [...r.allowlist].map(username => ({
      username,
      online: !!lobbyUsers[username] || r.members.some(m => m.username === username)
    }))
  }));
}

// Per-member room-info emit so banlist visibility can differ per recipient
// (creator always sees full banlist; other members see it only when
// banlistVisibility === 'all').
function emitRoomInfoToMembers(r, roomName) {
  for (const m of r.members) {
    const visibleBanlist = (m.username === r.creator || r.banlistVisibility === 'all')
      ? [...r.banlist] : null;
    io.to(m.socketId).emit('room-info', {
      creator:           r.creator,
      allowlist:         [...r.allowlist],
      tokenGate:         r.tokenGate || null,
      banlist:           visibleBanlist,
      banlistVisibility: r.banlistVisibility,
      spotlight:         r.spotlight || null
    });
  }
}

// v0.15 — clear the room's broadcast spotlight if `username` is the current
// target. Called whenever a member leaves (leave-room / disconnect / ban).
function clearSpotlightIfMember(r, room, username) {
  if (r.spotlight && r.spotlight === username) {
    r.spotlight = null;
    io.to(room).emit('room-spotlight-changed', { target: null, targetSocketId: null });
  }
}

function broadcastRooms() { io.emit('lobby-rooms', roomsSnapshot()); }


// ─────────────────────────────────────────────────────────────────────────────
// ── Rate Cache (10-minute TTL) ────────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────

const rateCache     = {}; // username → { rates, fetchedAt }
const RATE_CACHE_TTL = 10 * 60 * 1000;


// ─────────────────────────────────────────────────────────────────────────────
// ── Hive-Engine Token Balance (5-minute cache) ────────────────────────────────
// Checks if a caller holds a specific Hive-Engine token (PIZZA, NOBLEMAGE, etc.)
// Used for V2 TOKEN sections and ALLOW-IF-TOKEN bypass in BLOCKED section.
// ─────────────────────────────────────────────────────────────────────────────

const tokenBalanceCache = {}; // key: "account:SYMBOL" → { balance, fetchedAt }
const TOKEN_CACHE_TTL   = 5 * 60 * 1000; // 5 minutes — short enough to be responsive, long enough to avoid hammering the API

async function getHiveEngineTokenBalance(account, symbol) {
  const cacheKey = `${account}:${symbol}`;
  const cached   = tokenBalanceCache[cacheKey];
  if (cached && (Date.now() - cached.fetchedAt) < TOKEN_CACHE_TTL) {
    return cached.balance;
  }
  try {
    const res = await fetch('https://api.hive-engine.com/rpc/contracts', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({
        jsonrpc: '2.0',
        id:      1,
        method:  'find',
        params:  { contract: 'tokens', table: 'balances', query: { account, symbol } }
      }),
      signal: AbortSignal.timeout(8000)
    });
    if (!res.ok) {
      console.warn(`[token] Balance check ${symbol}/@${account} → HTTP ${res.status} — treating as 0 (not cached)`);
      return 0;
    }
    const data = await res.json();
    if (data.error) {
      console.warn(`[token] Balance check ${symbol}/@${account} → API error: ${JSON.stringify(data.error).slice(0, 200)} — treating as 0 (not cached)`);
      return 0;
    }
    if (!Array.isArray(data.result)) {
      console.warn(`[token] Balance check ${symbol}/@${account} → unexpected response shape: ${JSON.stringify(data).slice(0, 200)} — treating as 0 (not cached)`);
      return 0;
    }
    const balance = (data.result.length > 0)
      ? parseFloat(data.result[0].balance) || 0
      : 0;
    tokenBalanceCache[cacheKey] = { balance, fetchedAt: Date.now() };
    return balance;
  } catch(e) {
    console.warn(`[token] Balance check failed ${symbol}/@${account}: ${e.message} — treating as 0 (not cached)`);
    return 0; // safe fallback — treat as not holding the token
  }
}

// Clean expired token balance cache entries every 15 minutes
setInterval(() => {
  const cutoff = Date.now() - TOKEN_CACHE_TTL;
  for (const k in tokenBalanceCache) {
    if (tokenBalanceCache[k].fetchedAt < cutoff) delete tokenBalanceCache[k];
  }
}, 15 * 60 * 1000);


// ── v0.13 Hive Power lookup (owned HP only, for lobby post gate) ─────────────
const hpCache = {}; // username → { hp, fetchedAt }
const HP_CACHE_TTL = 5 * 60 * 1000;
let hivePerVestCache = { value: null, fetchedAt: 0 };
const HIVE_PER_VEST_TTL = 60 * 60 * 1000; // hive_per_vest moves slowly; 1h is fine

async function getHivePerVest() {
  if (hivePerVestCache.value && (Date.now() - hivePerVestCache.fetchedAt) < HIVE_PER_VEST_TTL) {
    return hivePerVestCache.value;
  }
  const data = await hivePost({
    jsonrpc: '2.0',
    method: 'condenser_api.get_dynamic_global_properties',
    params: [], id: 1
  });
  if (!data?.result) return null;
  const totalVesting = parseFloat(data.result.total_vesting_fund_hive);
  const totalVestingShares = parseFloat(data.result.total_vesting_shares);
  if (!totalVesting || !totalVestingShares) return null;
  const hivePerVest = totalVesting / totalVestingShares;
  hivePerVestCache = { value: hivePerVest, fetchedAt: Date.now() };
  return hivePerVest;
}

// Single account fetch populates both HP (computed from vesting_shares) and
// liquid HIVE (the `balance` field). v0.13 added the liquid-HIVE gate; doing
// both off one API call keeps the cache + Hive node load constant.
async function getAccountStats(username) {
  const cached = hpCache[username];
  if (cached && (Date.now() - cached.fetchedAt) < HP_CACHE_TTL) return cached.stats;
  const data = await hivePost({
    jsonrpc: '2.0',
    method: 'condenser_api.get_accounts',
    params: [[username]], id: 1
  });
  if (!data?.result?.[0]) {
    console.warn(`[hp] account @${username} not found — treating HP and HIVE as 0 (not cached)`);
    return { hp: 0, liquidHive: 0 };
  }
  const acct = data.result[0];
  const ownedVests = parseFloat(acct.vesting_shares); // "12345.678901 VESTS"
  const liquidHive = parseFloat(acct.balance);        // "33.000 HIVE"
  const hivePerVest = await getHivePerVest();
  if (!hivePerVest) {
    console.warn(`[hp] hive_per_vest unavailable — treating HP as 0 (HIVE balance still resolved, not cached)`);
    return { hp: 0, liquidHive };
  }
  const hp = ownedVests * hivePerVest;
  const stats = { hp, liquidHive };
  hpCache[username] = { stats, fetchedAt: Date.now() };
  return stats;
}

async function getHivePower(username)  { return (await getAccountStats(username)).hp; }
async function getLiquidHive(username) { return (await getAccountStats(username)).liquidHive; }

// Periodic cache cleanup (mirror tokenBalanceCache pattern)
setInterval(() => {
  const cutoff = Date.now() - HP_CACHE_TTL;
  for (const k in hpCache) if (hpCache[k].fetchedAt < cutoff) delete hpCache[k];
}, 15 * 60 * 1000);

async function checkLobbyPostGate(username) {
  // Fast-path when no gate is configured — no Hive API calls per message.
  if (LOBBY_POST_MIN_HP <= 0 && LOBBY_POST_MIN_HIVE <= 0 && !LOBBY_POST_MIN_TOKEN_SYMBOL) {
    return { allowed: true };
  }
  // If HP and/or HIVE is configured, fetch the account once and reuse both.
  let stats = null;
  if (LOBBY_POST_MIN_HP > 0 || LOBBY_POST_MIN_HIVE > 0) {
    stats = await getAccountStats(username);
  }
  const checks = [];
  if (LOBBY_POST_MIN_HP > 0) {
    checks.push({ kind: 'hp', actual: stats.hp, required: LOBBY_POST_MIN_HP, pass: stats.hp >= LOBBY_POST_MIN_HP });
  }
  if (LOBBY_POST_MIN_HIVE > 0) {
    checks.push({ kind: 'hive', actual: stats.liquidHive, required: LOBBY_POST_MIN_HIVE, pass: stats.liquidHive >= LOBBY_POST_MIN_HIVE });
  }
  if (LOBBY_POST_MIN_TOKEN_SYMBOL) {
    const tokenBal = await getHiveEngineTokenBalance(username, LOBBY_POST_MIN_TOKEN_SYMBOL);
    checks.push({
      kind: 'token', symbol: LOBBY_POST_MIN_TOKEN_SYMBOL,
      actual: tokenBal, required: LOBBY_POST_MIN_TOKEN_AMOUNT,
      pass: tokenBal >= LOBBY_POST_MIN_TOKEN_AMOUNT
    });
  }
  const passed = LOBBY_POST_GATE_MODE === 'and'
    ? checks.every(c => c.pass)
    : checks.some(c => c.pass);
  if (passed) return { allowed: true };
  const fmtRequired = c => c.kind === 'hp'   ? `${c.required} HP`
                         : c.kind === 'hive' ? `${c.required} HIVE`
                         :                     `${c.required} ${c.symbol}`;
  const fmtActual   = c => c.kind === 'hp'   ? `${c.actual.toFixed(1)} HP`
                         : c.kind === 'hive' ? `${c.actual.toFixed(3)} HIVE`
                         :                     `${c.actual} ${c.symbol}`;
  const required = checks.map(fmtRequired).join(LOBBY_POST_GATE_MODE === 'and' ? ' AND ' : ' OR ');
  const actual   = checks.map(fmtActual).join(', ');
  return {
    allowed: false,
    message: `This server requires ${required} to post in the lobby. You have ${actual}.`
  };
}


// ─────────────────────────────────────────────────────────────────────────────
// ── Hive Post Fetch (multi-node fallback) ─────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────

async function fetchRates(username) {
  const cached = rateCache[username];
  if (cached && (Date.now() - cached.fetchedAt) < RATE_CACHE_TTL) {
    return cached.rates;
  }
  try {
    // ── Step 1: Blog search first ────────────────────────────────────────────
    // The V2 rate editor posts with a timestamped permlink (v4call-rates-TIMESTAMP)
    // rather than the fixed permlink 'v4call-rates' used by V1.
    // Blog search sorted by date DESC ensures we always get the MOST RECENT
    // rates post, regardless of which permlink was used.
    const blogRes  = await fetch(HIVE_API, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({
        jsonrpc: '2.0', method: 'condenser_api.get_discussions_by_author_before_date',
        params:  [username, '', '2099-01-01T00:00:00', 20], id: 1
      })
    });
    const blogData = await blogRes.json();
    if (blogData.result) {
      // find() on a date-DESC list returns the most recent matching post first
      const post = blogData.result.find(p =>
        p.title.toLowerCase() === 'v4call-rates' && p.author === username
      );
      if (post) {
        const rates = parseRates(post.body);
        if (rates) {
          rateCache[username] = { rates, fetchedAt: Date.now() };
          console.log(`[rates] Loaded V${rates.version} for @${username} (permlink: ${post.permlink})`);
          return rates;
        }
      }
    }

    // ── Step 2: Fallback — try the fixed permlink 'v4call-rates' ─────────────
    // This catches V1 users who posted before the rate editor added timestamps,
    // and any edge case where the blog search missed the post.
    const directRes  = await fetch(HIVE_API, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({
        jsonrpc: '2.0', method: 'condenser_api.get_content',
        params:  [username, 'v4call-rates'], id: 1
      })
    });
    const directData = await directRes.json();
    if (directData.result && directData.result.author === username && directData.result.body) {
      const rates = parseRates(directData.result.body);
      if (rates) {
        rateCache[username] = { rates, fetchedAt: Date.now() };
        console.log(`[rates] Loaded V${rates.version} for @${username} (fallback direct permlink)`);
        return rates;
      }
    }

    console.log(`[rates] No v4call-rates post found for @${username}`);
    return null;
  } catch(e) {
    console.error(`[rates] Failed to fetch rates for @${username}:`, e.message);
    return null;
  }
}


// ─────────────────────────────────────────────────────────────────────────────
// ── Rate Parser — supports V1 and V2 format ───────────────────────────────────
//
// V1: [V4CALL-RATES-V1] ... [LIST:name] ... [/V4CALL-RATES-V1]
// V2: Adds [BLOCKED], [TOKEN:SYMBOL], TEXT-SESSION, CHAIN, SERVER, NOSTR fields
//
// The parser detects the version tag automatically — no manual switch needed.
// Old V1 posts continue to work unchanged.
// ─────────────────────────────────────────────────────────────────────────────

function parseRates(body) {
  // Result object — all fields have safe defaults
  const result = {
    version:     'V1',
    account:     '',
    chain:       'hive',
    server:      '',
    nostr:       '',
    platformFee: PLATFORM_FEE, // fall back to server default if not in post
    escrow:      ESCROW_ACCOUNT,
    blocked:     { users: [], message: 'You have been blocked.', allowIfToken: null },
    tokens:      [], // V2 only: [{ symbol, allowBlocked, text, voiceRing, voiceConnect, ... }]
    lists:       []  // named lists (family, friends, work, default, etc.)
  };

  // Detect version from opening tag
  if (/\[V4CALL-RATES-V2\]/i.test(body)) result.version = 'V2';

  // Extract the content between the outer block tags (accepts V1 or V2)
  const blockMatch = body.match(/\[V4CALL-RATES-V[12]\]([\s\S]*?)\[\/V4CALL-RATES-V[12]\]/i);
  if (!blockMatch) return null;
  const block = blockMatch[1];

  // ── Top-level identity / config fields ──────────────────────────────────────
  const accountM = block.match(/^ACCOUNT:(.+)$/mi);
  if (accountM) result.account = accountM[1].trim().toLowerCase();

  const chainM = block.match(/^CHAIN:(.+)$/mi);
  if (chainM)   result.chain   = chainM[1].trim().toLowerCase();

  const serverM = block.match(/^SERVER:(.+)$/mi);
  if (serverM)  result.server  = serverM[1].trim();

  const nostrM = block.match(/^NOSTR:(.+)$/mi);
  if (nostrM)   result.nostr   = nostrM[1].trim();

  const feeM = block.match(/^PLATFORM-FEE:(\d+(?:\.\d+)?)%/mi);
  if (feeM)     result.platformFee = parseFloat(feeM[1]) / 100;

  const escrowM = block.match(/^ESCROW:(.+)$/mi);
  if (escrowM)  result.escrow  = escrowM[1].trim();

  // ── [BLOCKED] section (V2 only — safely ignored if absent) ──────────────────
  const blockedMatch = block.match(/\[BLOCKED\]([\s\S]*?)\[\/BLOCKED\]/i);
  if (blockedMatch) {
    const b      = blockedMatch[1];
    const usersM = b.match(/^USERS:(.+)$/mi);
    if (usersM) {
      result.blocked.users = usersM[1]
        .split(',').map(u => u.trim().toLowerCase()).filter(Boolean);
    }
    const msgM = b.match(/^MESSAGE:(.+)$/mi);
    if (msgM) result.blocked.message = msgM[1].trim();

    const tokM = b.match(/^ALLOW-IF-TOKEN:(.+)$/mi);
    if (tokM) result.blocked.allowIfToken = tokM[1].trim().toUpperCase();
  }

  // ── [TOKEN:SYMBOL] sections (V2 only) ───────────────────────────────────────
  // Each token section defines rates that apply if the caller holds that token.
  // The first token section where the caller has a balance > 0 wins.
  const tokenRegex = /\[TOKEN:([^\]]+)\]([\s\S]*?)\[\/TOKEN\]/gi;
  let tokenMatch;
  while ((tokenMatch = tokenRegex.exec(block)) !== null) {
    const symbol      = tokenMatch[1].trim().toUpperCase();
    const tBody       = tokenMatch[2];
    const rates       = parseRateBlock(tBody);
    const allowBlocked = /^ALLOW-BLOCKED:\s*yes/mi.test(tBody); // can this token bypass a block?
    result.tokens.push({ symbol, allowBlocked, ...rates });
  }

  // ── [LIST:name] sections (V1 and V2) ────────────────────────────────────────
  // Each list has an optional USERS: line and one or more time windows.
  // [LIST:default] is the fallback for anyone not matched above.
  const listRegex = /\[LIST:([^\]]+)\]([\s\S]*?)\[\/LIST\]/gi;
  let listMatch;
  while ((listMatch = listRegex.exec(block)) !== null) {
    const listName = listMatch[1].trim().toLowerCase();
    const listBody = listMatch[2];
    const list     = { name: listName, users: [], windows: [] };

    const usersM = listBody.match(/^USERS:(.+)$/mi);
    if (usersM) {
      list.users = usersM[1].split(',').map(u => u.trim().toLowerCase()).filter(Boolean);
    }

    // [DAYS:...][TIME:HH:MM-HH:MM] ... [/TIME] windows within this list
    const timeRegex = /\[DAYS:([^\]]+)\]\[TIME:([^\]]+)\]([\s\S]*?)\[\/TIME\]/gi;
    let timeM;
    while ((timeM = timeRegex.exec(listBody)) !== null) {
      const timeParts = timeM[2].trim().split('-');
      const win = {
        days:      parseDays(timeM[1].trim()),
        timeStart: timeParts[0]?.trim() || '00:00',
        timeEnd:   timeParts[1]?.trim() || '23:59',
        ...parseRateBlock(timeM[3])
      };
      list.windows.push(win);
    }
    result.lists.push(list);
  }

  return result;
}

// ── parseRateBlock ────────────────────────────────────────────────────────────
// Parses TEXT, TEXT-SESSION, VOICE and VIDEO lines from inside a window or token block.
// Returns a flat object with all rate fields at safe 0 defaults.

function parseRateBlock(body) {
  const r = {
    invite:            0,
    text:              0,
    textSession:       0,
    voiceRing:         0, voiceConnect:    0, voiceRate:         0,
    voiceMinDepositMin: 10, voiceMinDepositHbd: null,
    videoRing:         0, videoConnect:    0, videoRate:         0,
    videoMinDepositMin: 10, videoMinDepositHbd: null
  };

  const inviteM = body.match(/^INVITE:(.+)$/mi);
  if (inviteM) r.invite = parseHbdOrFree(inviteM[1]);

  const textM = body.match(/^TEXT:(.+)$/mi);
  if (textM) r.text = parseHbdOrFree(textM[1]);

  const tsM = body.match(/^TEXT-SESSION:(.+)$/mi);
  if (tsM) r.textSession = parseHbdOrFree(tsM[1]);

  const voiceM = body.match(/^VOICE:(.+)$/mi);
  if (voiceM) Object.assign(r, parseVoiceVideoLine(voiceM[1], 'voice'));

  const videoM = body.match(/^VIDEO:(.+)$/mi);
  if (videoM) Object.assign(r, parseVoiceVideoLine(videoM[1], 'video'));

  return r;
}

// ── parseVoiceVideoLine ───────────────────────────────────────────────────────
// Parses a VOICE: or VIDEO: rate line into named fields.
// prefix is 'voice' or 'video'.
// Handles both numeric values and the FREE keyword.

function parseVoiceVideoLine(line, prefix) {
  const obj    = {};
  const isFree = (v) => v && v.toUpperCase() === 'FREE';

  const ringM    = line.match(/RING:([\d.]+|FREE)/i);
  if (ringM)    obj[`${prefix}Ring`]    = isFree(ringM[1])    ? 0 : parseFloat(ringM[1]);

  const connectM = line.match(/CONNECT:([\d.]+|FREE)/i);
  if (connectM) obj[`${prefix}Connect`] = isFree(connectM[1]) ? 0 : parseFloat(connectM[1]);

  const rateM    = line.match(/RATE:([\d.]+|FREE)/i);
  if (rateM)    obj[`${prefix}Rate`]    = isFree(rateM[1])    ? 0 : parseFloat(rateM[1]);

  // MIN-DEPOSIT can be: MIN-DEPOSIT:10min  or  MIN-DEPOSIT:0.500 HBD
  const minDepM  = line.match(/MIN-DEPOSIT:([\d.]+)\s*(min|HBD)/i);
  if (minDepM) {
    const val  = parseFloat(minDepM[1]);
    const unit = minDepM[2].toUpperCase();
    if (unit === 'HBD') obj[`${prefix}MinDepositHbd`] = val;
    else                obj[`${prefix}MinDepositMin`]  = val;
  }

  return obj;
}

// ── parseHbdOrFree ────────────────────────────────────────────────────────────
// Extracts a numeric HBD value from a string, treating FREE and 0 as 0.

function parseHbdOrFree(str) {
  const s = str.trim().toUpperCase();
  // Handle FREE and FREE/hr (rate editor appends /hr on session fields)
  if (s === 'FREE' || s.startsWith('FREE/')) return 0;
  const m = str.match(/([\d.]+)/);
  return m ? parseFloat(m[1]) : 0;
}

// ── parseDays ─────────────────────────────────────────────────────────────────
// Converts a DAYS string into an array of lowercase day abbreviations.
// Supports: 'mon-sun', 'mon-fri', 'sat-sun', or comma-separated 'mon,wed,fri'

function parseDays(daysStr) {
  const ALL = ['mon','tue','wed','thu','fri','sat','sun'];
  if (daysStr === 'mon-sun') return [...ALL];
  if (daysStr === 'mon-fri') return ALL.slice(0, 5); // ['mon','tue','wed','thu','fri']
  if (daysStr === 'sat-sun') return ['sat', 'sun'];
  return daysStr.split(',').map(d => d.trim().toLowerCase());
}

// ── timeInWindow ──────────────────────────────────────────────────────────────
// Returns true if timeStr (HH:MM) falls within [start, end].
// Handles windows that cross midnight (e.g. 23:00-07:00).

function timeInWindow(timeStr, start, end) {
  if (start <= end) return timeStr >= start && timeStr <= end;
  return timeStr >= start || timeStr <= end; // crosses midnight
}

// ── calcMinDeposit ────────────────────────────────────────────────────────────
// Returns the HBD amount a caller must pre-pay before a call connects.
// Fixed HBD amount takes priority over minutes-based calculation.

function calcMinDeposit(ratePerHour, minDepositMin, minDepositHbd) {
  if (minDepositHbd && minDepositHbd > 0) return minDepositHbd;
  const minutes = minDepositMin || 10; // default: 10 minutes
  return parseFloat(((ratePerHour / 60) * minutes).toFixed(3));
}

// ── buildCallRateResult ───────────────────────────────────────────────────────
// Converts a raw parsed rate block (window or token block) into the response
// object shape that index.html and the payment handlers expect.
// Keeps the API contract stable regardless of whether rates come from a
// time window or a token section.

function buildCallRateResult(rateBlock, callType, escrow, platformFee) {
  if (callType === 'text') {
    return {
      type:        'text',
      flat:        rateBlock.text        || 0,
      textSession: rateBlock.textSession || 0,
      escrow,
      platformFee
    };
  }
  if (callType === 'invite') {
    // Room-invite rate — flat per-invite fee. Same shape as text so it threads
    // through computePaymentOptions / isOptionBelowFloor unchanged.
    return {
      type:        'invite',
      flat:        rateBlock.invite || 0,
      escrow,
      platformFee
    };
  }
  const prefix      = callType === 'video' ? 'video' : 'voice';
  const ratePerHour = rateBlock[`${prefix}Rate`]    || 0;
  const minDeposit  = calcMinDeposit(
    ratePerHour,
    rateBlock[`${prefix}MinDepositMin`],
    rateBlock[`${prefix}MinDepositHbd`]
  );
  return {
    type:          callType,
    ring:          rateBlock[`${prefix}Ring`]    || 0,
    connect:       rateBlock[`${prefix}Connect`] || 0,
    rate:          ratePerHour,
    minDeposit,
    minDepositMin: rateBlock[`${prefix}MinDepositMin`] || 10,
    escrow,
    platformFee
  };
}


// ─────────────────────────────────────────────────────────────────────────────
// ── Rate Resolver — 5-step priority (V2) / 3-step (V1 fallback) ───────────────
//
// Priority order:
//   1. BLOCKED list  → block (unless caller holds ALLOW-IF-TOKEN token)
//   2. TOKEN sections → apply token rates if caller holds the token
//   3. Named lists    → first list containing the caller's username
//   4. Default list   → fallback for everyone else
//   5. No rates found → free call
//
// Returns: { type, ring, connect, rate, minDeposit, escrow, platformFee }
//          { blocked: true, message: '...' }
//          null  (free call)
//
// This function is async because token checks hit the Hive-Engine API.
// All call sites must await it.
// ─────────────────────────────────────────────────────────────────────────────

async function getRatesForCaller(calleeRates, callerUsername, callType = 'voice', now = new Date()) {
  if (!calleeRates) return null;

  const caller  = callerUsername.toLowerCase();
  const dayName = ['sun','mon','tue','wed','thu','fri','sat'][now.getDay()];
  const timeStr = now.getHours().toString().padStart(2,'0') + ':' + now.getMinutes().toString().padStart(2,'0');
  const { escrow } = calleeRates;

  // ── Platform fee enforcement ───────────────────────────────────────────────
  // Callee's posted fee must meet or exceed the server's minimum.
  // If it does, callee gets the best price (server's minimum, not their higher posted fee).
  const calleeFee = calleeRates.platformFee;
  const serverFee = PLATFORM_FEE;
  const platformFee = serverFee; // always charge the server's rate (best price for callee)

  if (typeof calleeFee === 'number' && calleeFee < serverFee) {
    const calleePct = (calleeFee * 100).toFixed(1);
    const serverPct = (serverFee * 100).toFixed(1);
    return {
      feeRejected: true,
      message: `@${calleeRates.account || 'this user'}'s platform fee (${calleePct}%) is below this server's minimum (${serverPct}%). They need to set PLATFORM-FEE to at least ${serverPct}% in their rates post to receive paid contacts on this server.`
    };
  }

  // ── Step 1: BLOCKED check ──────────────────────────────────────────────────
  if (calleeRates.blocked && calleeRates.blocked.users.includes(caller)) {
    const bypassToken = calleeRates.blocked.allowIfToken;
    let bypassed      = false;

    if (bypassToken) {
      const bal = await getHiveEngineTokenBalance(caller, bypassToken);
      if (bal > 0) {
        console.log(`[rates] Block bypass: @${caller} holds ${bypassToken} (bal: ${bal})`);
        bypassed = true;
        // If bypassed, apply that token's rates directly (skip normal token loop)
        const tokSection = (calleeRates.tokens || []).find(t => t.symbol === bypassToken);
        if (tokSection) {
          console.log(`[rates] Applying ${bypassToken} token rates for bypassed @${caller}`);
          const result = { currency: bypassToken, ...buildCallRateResult(tokSection, callType, escrow, platformFee) };
          if (isOptionBelowFloor(result)) {
            console.log(`[rates] @${caller} ${bypassToken} rate below floor (${RATE_FLOOR}) — treating as free`);
            return null;
          }
          return result;
        }
      }
    }

    if (!bypassed) {
      return { blocked: true, message: calleeRates.blocked.message || 'You have been blocked.' };
    }
    // If bypassed but no matching token section, fall through to list matching
  }

  // ── Step 2: TOKEN sections ─────────────────────────────────────────────────
  // First token section where the caller holds a balance > 0 wins.
  for (const tok of (calleeRates.tokens || [])) {
    const bal = await getHiveEngineTokenBalance(caller, tok.symbol);
    if (bal > 0) {
      console.log(`[rates] @${caller} qualifies for ${tok.symbol} token rates (bal: ${bal})`);
      const result = { currency: tok.symbol, ...buildCallRateResult(tok, callType, escrow, platformFee) };
      if (isOptionBelowFloor(result)) {
        console.log(`[rates] @${caller} ${tok.symbol} rate below floor (${RATE_FLOOR}) — treating as free`);
        return null;
      }
      return result;
    }
  }

  // ── Step 3: Named lists (first match wins, default excluded) ──────────────
  for (const list of (calleeRates.lists || [])) {
    if (list.name === 'default') continue;
    if (!list.users.includes(caller)) continue;
    const win = list.windows.find(w =>
      w.days.includes(dayName) && timeInWindow(timeStr, w.timeStart, w.timeEnd)
    );
    if (win) {
      console.log(`[rates] @${caller} matched list "${list.name}"`);
      const result = { currency: 'HBD', ...buildCallRateResult(win, callType, escrow, platformFee) };
      if (isOptionBelowFloor(result)) {
        console.log(`[rates] @${caller} HBD rate from list "${list.name}" below floor (${RATE_FLOOR}) — treating as free`);
        return null;
      }
      return result;
    }
  }

  // ── Step 4: Default list ───────────────────────────────────────────────────
  const defList = (calleeRates.lists || []).find(l => l.name === 'default');
  if (defList) {
    const win = defList.windows.find(w =>
      w.days.includes(dayName) && timeInWindow(timeStr, w.timeStart, w.timeEnd)
    );
    if (win) {
      const result = { currency: 'HBD', ...buildCallRateResult(win, callType, escrow, platformFee) };
      if (isOptionBelowFloor(result)) {
        console.log(`[rates] @${caller} HBD rate from default list below floor (${RATE_FLOOR}) — treating as free`);
        return null;
      }
      return result;
    }
  }

  // ── Step 5: No rates apply → free call ────────────────────────────────────
  return null;
}


// ─────────────────────────────────────────────────────────────────────────────
// ── isOptionBelowFloor ────────────────────────────────────────────────────────
// True if the option has at least one nonzero rate field below RATE_FLOOR.
// Such options can't be processed end-to-end (validator threshold + disbursement
// rounding both kick in at 0.001), so they're filtered from the picker and
// treated as free by the resolver.
function isOptionBelowFloor(opt) {
  if (!opt) return false;
  if (opt.type === 'text' || opt.type === 'invite') {
    return opt.flat > 0 && opt.flat < RATE_FLOOR;
  }
  // voice or video
  return [opt.ring, opt.connect, opt.rate].some(v => v > 0 && v < RATE_FLOOR);
}

// ── computePaymentOptions ─────────────────────────────────────────────────────
// Returns ALL the payment options a caller qualifies for (one per accepted
// currency: any token section where the caller has balance, plus an HBD option
// if a named/default list window matches). Used by the multi-currency picker
// AND by v0.16.6 recipient-side rate enforcement on federation.
//
// Why a helper: getRatesForCaller returns a single applicable (first token
// match wins, then HBD). The picker shows MULTIPLE options so the caller can
// choose which currency to pay in. Validating "paid >= rate" on the recipient
// side requires looking up the rate for the currency the caller ACTUALLY
// paid in — not just the resolver's first pick.
//
// Returns: { options, blocked, feeRejected, message }
//   options: [{ currency, balance?, ...buildCallRateResult fields }]
//   blocked / feeRejected: true if the caller is blocked / fee minimum not met
//   message: human-readable reason when blocked or feeRejected
// ─────────────────────────────────────────────────────────────────────────────
async function computePaymentOptions(rates, callerUsername, callType, now = new Date()) {
  if (!rates) return { options: [], blocked: false, feeRejected: false };

  const caller  = callerUsername.toLowerCase();
  const dayName = ['sun','mon','tue','wed','thu','fri','sat'][now.getDay()];
  const timeStr = now.getHours().toString().padStart(2,'0') + ':' + now.getMinutes().toString().padStart(2,'0');
  const escrow  = rates.escrow;

  // Platform fee minimum — server rejects if callee's posted fee is too low
  const calleeFee   = rates.platformFee;
  const serverFee   = PLATFORM_FEE;
  const platformFee = serverFee;
  if (typeof calleeFee === 'number' && calleeFee < serverFee) {
    return {
      options: [],
      blocked: false,
      feeRejected: true,
      message: `@${rates.account || 'this user'}'s platform fee (${(calleeFee*100).toFixed(1)}%) is below this server's minimum (${(serverFee*100).toFixed(1)}%).`
    };
  }

  // Block-list — overrides everything unless caller holds the bypass token
  if (rates.blocked && rates.blocked.users.includes(caller)) {
    let bypassed = false;
    if (rates.blocked.allowIfToken) {
      const bal = await getHiveEngineTokenBalance(caller, rates.blocked.allowIfToken);
      if (bal > 0) bypassed = true;
    }
    if (!bypassed) {
      return {
        options: [],
        blocked: true,
        feeRejected: false,
        message: rates.blocked.message || 'You have been blocked.'
      };
    }
  }

  const options = [];
  let belowFloor = false;

  // Helper: push if above floor, otherwise flag belowFloor and skip.
  const pushOrFlag = (opt) => {
    if (isOptionBelowFloor(opt)) {
      belowFloor = true;
      console.log(`[rates] Option for ${opt.currency} below floor (${RATE_FLOOR}) — filtered`);
      return;
    }
    options.push(opt);
  };

  // Token sections — one option per token the caller holds
  for (const tok of (rates.tokens || [])) {
    const bal = await getHiveEngineTokenBalance(caller, tok.symbol);
    if (bal > 0) {
      pushOrFlag({
        currency: tok.symbol,
        balance:  bal,
        ...buildCallRateResult(tok, callType, escrow, platformFee)
      });
    }
  }

  // HBD option — first matching named list, else default list
  let hbdOption = null;
  for (const list of (rates.lists || [])) {
    if (list.name === 'default') continue;
    if (!list.users.includes(caller)) continue;
    const win = list.windows.find(w =>
      w.days.includes(dayName) && timeInWindow(timeStr, w.timeStart, w.timeEnd)
    );
    if (win) {
      hbdOption = { currency: 'HBD', listName: list.name, ...buildCallRateResult(win, callType, escrow, platformFee) };
      break;
    }
  }
  if (!hbdOption) {
    const defList = (rates.lists || []).find(l => l.name === 'default');
    if (defList) {
      const win = defList.windows.find(w =>
        w.days.includes(dayName) && timeInWindow(timeStr, w.timeStart, w.timeEnd)
      );
      if (win) {
        hbdOption = { currency: 'HBD', listName: 'default', ...buildCallRateResult(win, callType, escrow, platformFee) };
      }
    }
  }
  if (hbdOption) pushOrFlag(hbdOption);

  return { options, blocked: false, feeRejected: false, belowFloor };
}


// ─────────────────────────────────────────────────────────────────────────────
// ── Paid-invite helpers (v0.16.10) ────────────────────────────────────────────
// Mirrors the paid-DM pattern but on the 'invite' callType. Returns the picker
// shape (an array of currency options the inviter qualifies for) or a reason
// the invitee can't be invited at all (blocked / platform-fee rejected).
// Free invite → options.length === 0 + no block/fee reject = "go ahead, free".
// ─────────────────────────────────────────────────────────────────────────────
async function getInviteOptions(inviteeUsername, inviterUsername) {
  const rates = await fetchRates(inviteeUsername);
  if (!rates) return { options: [], blocked: false, feeRejected: false, escrow: ESCROW_ACCOUNT };
  const res = await computePaymentOptions(rates, inviterUsername, 'invite', new Date());
  // computePaymentOptions returns ALL matching options (one per currency the
  // inviter qualifies for) — including ones with flat=0. For voice/video the
  // 0-rate option is meaningful (the picker still shows the currency); for
  // invite it just means "free in this currency". Filter zero-rate options
  // so options.length === 0 reliably means "free invite, no payment needed."
  const paidOptions = (res.options || []).filter(o => o.flat >= RATE_FLOOR);
  return { ...res, options: paidOptions, escrow: rates.escrow || ESCROW_ACCOUNT };
}

// Disburses an accepted paid invite: net to invitee, platform-fee to server.
async function disbursePaidInvite(inviteId) {
  const e = pendingPaidInvites[inviteId];
  if (!e || e.status !== 'pending') return;
  e.status = 'accepted';

  const platformFee  = PLATFORM_FEE;
  const platformCut  = parseFloat((e.paid * platformFee).toFixed(3));
  const inviteeNet   = parseFloat((e.paid - platformCut).toFixed(3));

  if (inviteeNet >= RATE_FLOOR) {
    const payoutMemo = `v4call:invite-payout:${inviteId}`;
    ledgerPayment(inviteId, 'invite_payout', ESCROW_ACCOUNT, e.invitee, inviteeNet, payoutMemo, 'pending', null, e.currency);
    sendFromEscrow(e.invitee, inviteeNet, payoutMemo, e.currency, inviteId).then(r => {
      if (r && r.success) {
        ledgerPaymentUpdate(inviteId, 'invite_payout', 'sent', r.txId);
        const lu = lobbyUsers[e.invitee];
        if (lu) io.to(lu.socketId).emit('lobby-info', {
          text: `💰 @${e.inviter} paid you ${inviteeNet.toFixed(3)} ${e.currency} for the invite to #${e.room}.`
        });
      } else {
        console.error(`[paid-invite] Payout to @${e.invitee} failed: ${r && r.reason}`);
        ledgerPaymentUpdate(inviteId, 'invite_payout', 'failed', null);
      }
    });
  }

  if (platformCut >= RATE_FLOOR) {
    const feeMemo = `v4call:invite-fee:${inviteId}`;
    ledgerPayment(inviteId, 'invite_fee', ESCROW_ACCOUNT, SERVER_HIVE_ACCOUNT, platformCut, feeMemo, 'pending', null, e.currency);
    sendFromEscrow(SERVER_HIVE_ACCOUNT, platformCut, feeMemo, e.currency, inviteId).then(r => {
      if (r && r.success) ledgerPaymentUpdate(inviteId, 'invite_fee', 'sent', r.txId);
    });
  }

  const lu = lobbyUsers[e.inviter];
  const inviteeLabel = e.invitee_server ? `@${e.invitee}@${e.invitee_server}` : `@${e.invitee}`;
  if (lu) io.to(lu.socketId).emit('lobby-info', {
    text: `✓ ${inviteeLabel} accepted your invite to #${e.room}. Net ${inviteeNet.toFixed(3)} ${e.currency} sent.`
  });
  console.log(`[paid-invite] ${inviteId} accepted — net ${inviteeNet} ${e.currency} → @${e.invitee}, fee ${platformCut} → @${SERVER_HIVE_ACCOUNT}`);
}

// ─────────────────────────────────────────────────────────────────────────────
// ── Payment tracking ──────────────────────────────────────────────────────────
// activePayments: callId → { caller, callee, ringPaid, depositPaid, creditRemaining,
//                            connectPaid, startTime, ratePerHour, escrow, platformFee, _processing }
// ─────────────────────────────────────────────────────────────────────────────

const activePayments = {};


// ─────────────────────────────────────────────────────────────────────────────
// ── Credit Burn Engine ────────────────────────────────────────────────────────
// Tracks remaining call credit and disconnects when exhausted.
// Fires warnings at 5 minutes and 2 minutes remaining.
// ─────────────────────────────────────────────────────────────────────────────

const creditTimers = {}; // callId → intervalId

function startCreditBurn(callId, roomName) {
  const payment = activePayments[callId];
  if (!payment || !payment.ratePerHour) return; // free call — nothing to burn

  const ratePerMs = payment.ratePerHour / (60 * 60 * 1000);
  let warned5     = false;
  let warned2     = false;

  creditTimers[callId] = setInterval(async () => {
    const p = activePayments[callId];
    if (!p) { clearInterval(creditTimers[callId]); return; }

    const elapsed         = Date.now() - (p.startTime || Date.now());
    const burned          = elapsed * ratePerMs;
    p.creditRemaining     = Math.max(0, (p.depositPaid || 0) - burned);
    const minLeft         = p.creditRemaining / (p.ratePerHour / 60);

    if (!warned5 && minLeft <= 5 && minLeft > 2) {
      warned5 = true;
      io.to(roomName).emit('credit-warning', {
        minutesLeft: parseFloat(minLeft.toFixed(1)),
        creditLeft:  parseFloat(p.creditRemaining.toFixed(3)),
        level:       '5min'
      });
    }

    if (!warned2 && minLeft <= 2 && minLeft > 0) {
      warned2 = true;
      io.to(roomName).emit('credit-warning', {
        minutesLeft: parseFloat(minLeft.toFixed(1)),
        creditLeft:  parseFloat(p.creditRemaining.toFixed(3)),
        level:       '2min'
      });
    }

    if (p.creditRemaining <= 0) {
      clearInterval(creditTimers[callId]);
      delete creditTimers[callId];
      console.log(`[credit] Call ${callId} ran out of credit — disconnecting`);
      io.to(roomName).emit('credit-exhausted', { callId });
      await processCallEnd(callId, roomName, io, lobbyUsers, 'credit_exhausted');
    }
  }, 10000); // check every 10 seconds
}

function stopCreditBurn(callId) {
  if (creditTimers[callId]) {
    clearInterval(creditTimers[callId]);
    delete creditTimers[callId];
  }
}


// ─────────────────────────────────────────────────────────────────────────────
// ── Call Cooldown ─────────────────────────────────────────────────────────────
// Prevents spam-ringing the same user on free calls.
// Paid ring attempts bypass this — the ring fee is their skin in the game.
// ─────────────────────────────────────────────────────────────────────────────

const callCooldowns = {}; // "caller->callee" → timestamp of last attempt

function checkCallCooldown(caller, callee) {
  const key  = `${caller}->${callee}`;
  const last = callCooldowns[key] || 0;
  const age  = Date.now() - last;
  if (age < CALL_COOLDOWN_MS) {
    return { allowed: false, waitMs: CALL_COOLDOWN_MS - age };
  }
  callCooldowns[key] = Date.now();
  return { allowed: true };
}

// Clean up entries older than 5 minutes
setInterval(() => {
  const cutoff = Date.now() - 5 * 60 * 1000;
  for (const k in callCooldowns) {
    if (callCooldowns[k] < cutoff) delete callCooldowns[k];
  }
}, 60000);


// ─────────────────────────────────────────────────────────────────────────────
// ── Hive API Helper — tries multiple nodes ─────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────

async function hivePost(body, nodes = HIVE_API_NODES) {
  const method = body && body.method ? body.method : 'unknown';
  for (const node of nodes) {
    try {
      const res  = await fetch(node, {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify(body),
        signal:  AbortSignal.timeout(8000)
      });
      if (!res.ok) {
        console.warn(`[hive] ${node} → HTTP ${res.status} for ${method} — trying next`);
        continue;
      }
      const data = await res.json();
      if (data.result !== undefined) return data;
      // 200 OK but no result — log what we got back so the operator can see why
      // (Hive nodes that return JSON-RPC errors or empty bodies were silently
      // skipped before, making "discovery returned 0" look like a network bug.)
      const errMsg = data.error
        ? (typeof data.error === 'string' ? data.error : JSON.stringify(data.error).slice(0, 200))
        : 'no `result` field in response';
      const preview = JSON.stringify(data).slice(0, 200);
      console.warn(`[hive] ${node} returned 200 but no result for ${method}: ${errMsg} — body: ${preview} — trying next`);
    } catch(e) {
      console.warn(`[hive] Node ${node} failed: ${e.message} — trying next`);
    }
  }
  console.warn(`[hive] All nodes exhausted for ${method} (tried ${nodes.length})`);
  return null;
}


// ─────────────────────────────────────────────────────────────────────────────
// ── Escrow Helpers ────────────────────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────

async function getEscrowBalance() {
  const data = await hivePost({
    jsonrpc: '2.0', method: 'condenser_api.get_accounts',
    params: [[ESCROW_ACCOUNT]], id: 1
  });
  if (!data?.result?.[0]) return 0;
  const hbd = data.result[0].hbd_balance || data.result[0].sbd_balance || '0 HBD';
  return parseFloat(hbd.split(' ')[0]);
}

async function sendFromEscrow(to, amount, memo, currency = 'HBD', callId = null) {
  // Route token transfers to the Hive-Engine path
  if (currency !== 'HBD' && currency !== 'HIVE') {
    return sendFromEscrowToken(to, amount, memo, currency, callId);
  }

  const escrowKey = process.env.V4CALL_ESCROW_KEY;
  if (!escrowKey) {
    console.error('[escrow] V4CALL_ESCROW_KEY not set — cannot disburse');
    if (callId) ledgerPayment(callId, 'payout', ESCROW_ACCOUNT, to, amount, memo, 'failed');
    return { success: false, reason: 'Escrow key not configured' };
  }
  if (amount < 0.001) {
    console.log(`[escrow] Amount ${amount} too small — skipping transfer to @${to}`);
    return { success: true, skipped: true };
  }

  const balance = await getEscrowBalance();
  if (balance < amount) {
    console.error(`[escrow] Insufficient balance: have ${balance} HBD, need ${amount} HBD for @${to}`);
    if (callId) ledgerPayment(callId, 'payout', ESCROW_ACCOUNT, to, amount, memo, 'failed', null);
    return { success: false, reason: `Escrow balance insufficient (${balance.toFixed(3)} HBD available)` };
  }

  try {
    const client    = new dhive.Client(HIVE_API_NODES);
    const key       = dhive.PrivateKey.fromString(escrowKey);
    const amountStr = amount.toFixed(3) + ' ' + currency;

    console.log(`[escrow] Transfer: ${amountStr} → @${to} | memo: ${memo}`);
    const result = await client.broadcast.transfer({
      from: ESCROW_ACCOUNT, to, amount: amountStr, memo
    }, key);

    console.log(`[escrow] ✓ Sent ${amountStr} to @${to} — tx: ${result.id}`);
    if (callId) ledgerPayment(callId, 'payout', ESCROW_ACCOUNT, to, amount, memo, 'sent', result.id);
    return { success: true, txId: result.id, amount: amountStr };
  } catch(e) {
    console.error(`[escrow] ✗ TRANSFER FAILED to @${to}: ${e.message}`);
    if (callId) ledgerPayment(callId, 'payout', ESCROW_ACCOUNT, to, amount, memo, 'failed');
    return { success: false, reason: e.message };
  }
}


// ─────────────────────────────────────────────────────────────────────────────
// ── Hive-Engine Token Transfers (for custom token payments) ──────────────────
// ─────────────────────────────────────────────────────────────────────────────

async function getEscrowTokenBalance(symbol) {
  try {
    const bal = await getHiveEngineTokenBalance(ESCROW_ACCOUNT, symbol);
    return bal;
  } catch(e) {
    console.error(`[escrow-token] Balance check failed for ${symbol}: ${e.message}`);
    return 0;
  }
}

async function sendFromEscrowToken(to, amount, memo, symbol, callId = null) {
  const escrowKey = process.env.V4CALL_ESCROW_KEY;
  if (!escrowKey) {
    console.error('[escrow-token] V4CALL_ESCROW_KEY not set — cannot disburse');
    if (callId) ledgerPayment(callId, 'payout', ESCROW_ACCOUNT, to, amount, memo, 'failed');
    return { success: false, reason: 'Escrow key not configured' };
  }
  if (amount < 0.001) {
    console.log(`[escrow-token] Amount ${amount} too small — skipping transfer to @${to}`);
    return { success: true, skipped: true };
  }

  const balance = await getEscrowTokenBalance(symbol);
  if (balance < amount) {
    console.error(`[escrow-token] Insufficient ${symbol} balance: have ${balance}, need ${amount} for @${to}`);
    if (callId) ledgerPayment(callId, 'payout', ESCROW_ACCOUNT, to, amount, memo, 'failed');
    return { success: false, reason: `Escrow ${symbol} balance insufficient (${balance} available)` };
  }

  try {
    const client = new dhive.Client(HIVE_API_NODES);
    const key    = dhive.PrivateKey.fromString(escrowKey);
    const json   = JSON.stringify({
      contractName:   'tokens',
      contractAction: 'transfer',
      contractPayload: {
        symbol,
        to,
        quantity: amount.toFixed(8).replace(/\.?0+$/, ''), // Hive-Engine uses variable precision
        memo
      }
    });

    console.log(`[escrow-token] Transfer: ${amount} ${symbol} → @${to} | memo: ${memo}`);
    const result = await client.broadcast.json({
      required_auths:         [ESCROW_ACCOUNT],
      required_posting_auths: [],
      id:                     'ssc-mainnet-hive',
      json
    }, key);

    console.log(`[escrow-token] ✓ Sent ${amount} ${symbol} to @${to} — tx: ${result.id}`);
    if (callId) ledgerPayment(callId, 'payout', ESCROW_ACCOUNT, to, amount, memo, 'sent', result.id);
    return { success: true, txId: result.id, amount: `${amount} ${symbol}` };
  } catch(e) {
    console.error(`[escrow-token] ✗ TOKEN TRANSFER FAILED to @${to}: ${e.message}`);
    if (callId) ledgerPayment(callId, 'payout', ESCROW_ACCOUNT, to, amount, memo, 'failed');
    return { success: false, reason: e.message };
  }
}

async function verifyHiveEnginePayment(fromUser, toUser, amount, symbol, memo, retries = PAYMENT_VERIFY_RETRIES) {
  // Hive-Engine doesn't expose transferHistory via the contracts RPC reliably.
  // Instead, we verify by checking that the escrow account holds enough of the token.
  // The payment was already signed via Keychain — we trust the client-side broadcast
  // succeeded if the escrow balance reflects the expected amount.
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const bal = await getHiveEngineTokenBalance(toUser, symbol);
      if (bal >= amount - 0.001) {
        console.log(`[payment] ✓ Verified ${symbol} balance on @${toUser}: ${bal} (need ${amount}) — attempt ${attempt}`);
        // Clear the balance cache so next check is fresh
        delete tokenBalanceCache[`${toUser}:${symbol}`];
        return true;
      }
      console.warn(`[payment] HE verify attempt ${attempt}/${retries} — @${toUser} ${symbol} balance: ${bal}, need: ${amount}`);
      // Clear cache to get fresh balance on next attempt
      delete tokenBalanceCache[`${toUser}:${symbol}`];
    } catch(e) {
      console.warn(`[payment] HE verify attempt ${attempt}/${retries} error:`, e.message);
    }
    if (attempt < retries) await new Promise(r => setTimeout(r, PAYMENT_VERIFY_DELAY_MS));
  }
  console.error(`[payment] ✗ HE payment verification failed after ${retries} attempts`);
  return false;
}


// ─────────────────────────────────────────────────────────────────────────────
// ── Call End — calculate bill, disburse from escrow, send receipts ────────────
// ─────────────────────────────────────────────────────────────────────────────

async function processCallEnd(callId, roomName, io, lobbyUsers, endReason = 'unknown') {
  const payment = activePayments[callId];
  if (!payment) {
    console.warn(`[billing] processCallEnd called for unknown callId: ${callId}`);
    return;
  }
  if (payment._processing) {
    console.warn(`[billing] processCallEnd already running for ${callId} — skipping`);
    return;
  }
  payment._processing = true;
  stopCreditBurn(callId);

  // Federated call: we host the room but the callee's escrow holds the funds.
  // Hand off to the callee's server, which will disburse from its own escrow
  // and send back a receipt for our caller.
  const room = rooms[roomName];
  if (room?.federated && payment.calleeServer) {
    const peer = federationPeers[payment.calleeServer];
    const now        = Date.now();
    const durationMs = payment.startTime ? (now - payment.startTime) : 0;
    if (peer?.connected) {
      fedSend(peer.ws, {
        type: 'call-ended',
        callId,
        durationMs,
        endReason,
        callerServer: SERVER_DOMAIN
      });
      console.log(`[billing] Federated call ${callId} → handed off to ${payment.calleeServer} for disbursement`);
    } else {
      console.error(`[billing] ⚠ Federated call ${callId} ended but peer ${payment.calleeServer} unreachable — funds stuck in their escrow`);
    }
    ledgerCallUpdate(callId, {
      ended_at:     new Date(now).toISOString(),
      duration_min: parseFloat((durationMs / 60000).toFixed(2)),
      status:       'ended_federated',
      end_reason:   endReason
    });
    delete activePayments[callId];
    return;
  }

  const now         = Date.now();
  const startTime   = payment.startTime || now;
  const durationMs  = payment.startTime ? (now - startTime) : 0;
  const durationMin = Math.min(durationMs / 60000, MAX_CALL_DURATION_MIN);
  const durationHr  = durationMin / 60;

  const ratePerHour = payment.ratePerHour || 0;
  const depositPaid = payment.depositPaid || 0;
  const connectPaid = payment.connectPaid || 0;
  const ringPaid    = payment.ringPaid    || 0;
  const platformFee = payment.platformFee || 0.10;
  const currency    = payment.currency    || 'HBD';

  // ── Money flow ──────────────────────────────────────────────────────────────
  // Total received = ring + connect + deposit
  // ring fee       → platform (non-refundable interrupt cost)
  // connect fee    → callee (non-refundable answer fee, minus platform %)
  // deposit        → duration cost to callee; unused portion refunded to caller

  const durationCost   = parseFloat(Math.min(ratePerHour * durationHr, depositPaid).toFixed(3));
  const refundAmount   = parseFloat(Math.max(0, depositPaid - durationCost).toFixed(3));
  const calleeGross    = parseFloat((connectPaid + durationCost).toFixed(3));
  const platformOnCall = parseFloat((calleeGross * platformFee).toFixed(3));
  const calleeNet      = parseFloat((calleeGross - platformOnCall).toFixed(3));
  const platformTotal  = parseFloat((ringPaid + platformOnCall).toFixed(3));

  // Sanity check
  const totalIn  = ringPaid + connectPaid + depositPaid;
  const totalOut = calleeNet + refundAmount + platformTotal;
  const delta    = parseFloat((totalIn - totalOut).toFixed(3));
  if (Math.abs(delta) > 0.002) {
    console.warn(`[billing] ⚠ Accounting delta: in=${totalIn} out=${totalOut} diff=${delta} HBD`);
  }

  console.log(`[billing] Call ${callId} ended (${endReason})`);
  console.log(`[billing]   Duration:   ${durationMin.toFixed(2)} min`);
  console.log(`[billing]   Ring:       ${ringPaid} ${currency} → platform`);
  console.log(`[billing]   Connect:    ${connectPaid} ${currency} → callee`);
  console.log(`[billing]   Duration $: ${durationCost} ${currency} → callee`);
  console.log(`[billing]   Refund:     ${refundAmount} ${currency} → caller`);
  console.log(`[billing]   Platform:   ${platformTotal} ${currency}`);
  console.log(`[billing]   Callee net: ${calleeNet} ${currency}`);

  const receipt = {
    callId,
    caller: payment.caller, callee: payment.callee,
    startTime: new Date(startTime).toISOString(), endTime: new Date(now).toISOString(),
    durationMin: parseFloat(durationMin.toFixed(2)),
    ringPaid, connectPaid, depositPaid, durationCost, refundAmount,
    calleeNet, platformTotal, platformOnCall, currency, endReason
  };

  // Update SQLite ledger
  ledgerCallUpdate(callId, {
    ended_at:      new Date().toISOString(),
    duration_min:  parseFloat(durationMin.toFixed(2)),
    ring_paid:     ringPaid,
    connect_paid:  connectPaid,
    duration_cost: durationCost,
    callee_net:    calleeNet,
    platform_cut:  platformTotal,
    status:        'ended',
    end_reason:    endReason
  });

  const callerSid = lobbyUsers[payment.caller]?.socketId;
  const calleeSid = lobbyUsers[payment.callee]?.socketId;
  if (callerSid) io.to(callerSid).emit('call-receipt', { ...receipt, perspective: 'caller' });
  if (calleeSid) io.to(calleeSid).emit('call-receipt', { ...receipt, perspective: 'callee' });

  // ── Disburse from escrow ────────────────────────────────────────────────────

  // 1. Callee payout
  if (calleeNet >= 0.001) {
    const payoutMemo = `v4call:payout:${callId}:${durationMin.toFixed(1)}min`;
    ledgerPayment(callId, 'payout', ESCROW_ACCOUNT, payment.callee, calleeNet, payoutMemo, 'pending');
    const result = await sendFromEscrow(payment.callee, calleeNet, payoutMemo, currency, callId);
    if (!result.success) {
      console.error(`[billing] Callee payout FAILED to @${payment.callee}: ${result.reason}`);
      if (calleeSid) io.to(calleeSid).emit('payout-failed', { amount: calleeNet, reason: result.reason, callId });
    } else {
      ledgerPaymentUpdate(callId, 'payout', 'sent', result.txId);
    }
  }

  // 2. Refund unused deposit to caller
  if (refundAmount >= 0.001) {
    const refundMemo = `v4call:refund:${callId}:unused-credit`;
    ledgerPayment(callId, 'refund', ESCROW_ACCOUNT, payment.caller, refundAmount, refundMemo, 'pending');
    const refundResult = await sendFromEscrow(payment.caller, refundAmount, refundMemo, currency, callId);
    if (refundResult.success) {
      ledgerPaymentUpdate(callId, 'refund', 'sent', refundResult.txId);
      console.log(`[billing] Refunded ${refundAmount} ${currency} to @${payment.caller}`);
    } else {
      console.error(`[billing] Refund FAILED to @${payment.caller}: ${refundResult.reason}`);
      if (callerSid) io.to(callerSid).emit('payout-failed', {
        amount: refundAmount, reason: refundResult.reason, callId,
        message: 'Your unused credit refund could not be sent automatically'
      });
    }
  }

  // 3. Platform fee (ring fee + % cut)
  if (platformTotal >= 0.001) {
    const feeMemo   = `v4call:fee:${callId}:ring+cut`;
    ledgerPayment(callId, 'platform_fee', ESCROW_ACCOUNT, SERVER_HIVE_ACCOUNT, platformTotal, feeMemo, 'pending');
    const feeResult = await sendFromEscrow(SERVER_HIVE_ACCOUNT, platformTotal, feeMemo, currency, callId);
    if (feeResult.success) {
      ledgerPaymentUpdate(callId, 'platform_fee', 'sent', feeResult.txId);
      console.log(`[billing] Platform fee sent: ${platformTotal} ${currency}`);
    } else {
      console.error(`[billing] Platform fee FAILED: ${feeResult.reason}`);
    }
  }

  delete activePayments[callId];
}


// ─────────────────────────────────────────────────────────────────────────────
// ── Federated Call End — disburse on the callee's server ─────────────────────
//
// Runs on the CALLEE's server when the caller's server signals `call-ended`.
// Uses the payment state we've been accumulating from `payment-verified`
// messages, then disburses from OUR escrow (which is where the caller actually
// paid, since callee's rates post points at us):
//
//   callee-net   → local callee (on-chain transfer, our escrow is the source)
//   platform-cut → our SERVER_HIVE_ACCOUNT (platform fee goes to us)
//   refund       → remote caller (cross-server Hive transfer from our escrow)
//
// Sends a `call-receipt-fed` back to the caller's server so it can show the
// caller their receipt; emits `call-receipt` locally to the callee.
// ─────────────────────────────────────────────────────────────────────────────

async function processFederatedCallEnd(callId, durationMs, endReason, callerServer) {
  const payment = activePayments[callId];
  if (!payment) {
    console.warn(`[fed-billing] No payment state for ${callId} — skipping disbursement`);
    return;
  }
  if (payment._processing) return;
  payment._processing = true;

  const now         = Date.now();
  const durationMin = Math.min((durationMs || 0) / 60000, MAX_CALL_DURATION_MIN);
  const durationHr  = durationMin / 60;

  const ratePerHour = payment.ratePerHour || 0;
  const depositPaid = payment.depositPaid || 0;
  const connectPaid = payment.connectPaid || 0;
  const ringPaid    = payment.ringPaid    || 0;
  // Use OUR server's minimum, matching what the callee's rates post promised —
  // this is the "callee's server takes the platform fee" rule in Model 3.
  const platformFee = PLATFORM_FEE;
  const currency    = payment.currency    || 'HBD';

  const durationCost   = parseFloat(Math.min(ratePerHour * durationHr, depositPaid).toFixed(3));
  const refundAmount   = parseFloat(Math.max(0, depositPaid - durationCost).toFixed(3));
  const calleeGross    = parseFloat((connectPaid + durationCost).toFixed(3));
  const platformOnCall = parseFloat((calleeGross * platformFee).toFixed(3));
  const calleeNet      = parseFloat((calleeGross - platformOnCall).toFixed(3));
  const platformTotal  = parseFloat((ringPaid + platformOnCall).toFixed(3));

  console.log(`[fed-billing] Call ${callId} ended (${endReason}) — disbursing`);
  console.log(`[fed-billing]   Duration:   ${durationMin.toFixed(2)} min`);
  console.log(`[fed-billing]   Callee net: ${calleeNet} ${currency} → @${payment.callee}`);
  console.log(`[fed-billing]   Platform:   ${platformTotal} ${currency} → @${SERVER_HIVE_ACCOUNT}`);
  console.log(`[fed-billing]   Refund:     ${refundAmount} ${currency} → @${payment.caller}@${callerServer}`);

  const receipt = {
    callId,
    caller: payment.caller, callee: payment.callee,
    startTime: payment.startTime ? new Date(payment.startTime).toISOString() : null,
    endTime:   new Date(now).toISOString(),
    durationMin: parseFloat(durationMin.toFixed(2)),
    ringPaid, connectPaid, depositPaid, durationCost, refundAmount,
    calleeNet, platformTotal, platformOnCall, currency, endReason,
    federated: true, callerServer
  };

  ledgerCallUpdate(callId, {
    ended_at:      new Date(now).toISOString(),
    duration_min:  parseFloat(durationMin.toFixed(2)),
    ring_paid:     ringPaid,
    connect_paid:  connectPaid,
    duration_cost: durationCost,
    callee_net:    calleeNet,
    platform_cut:  platformTotal,
    status:        'ended',
    end_reason:    endReason
  });

  // 1. Callee payout (local user, our escrow)
  if (calleeNet >= 0.001) {
    const payoutMemo = `v4call:payout:${callId}:${durationMin.toFixed(1)}min`;
    ledgerPayment(callId, 'payout', ESCROW_ACCOUNT, payment.callee, calleeNet, payoutMemo, 'pending');
    const r = await sendFromEscrow(payment.callee, calleeNet, payoutMemo, currency, callId);
    if (r.success) ledgerPaymentUpdate(callId, 'payout', 'sent', r.txId);
    else           console.error(`[fed-billing] Payout FAILED: ${r.reason}`);
  }

  // 2. Refund to remote caller (our escrow → their Hive account)
  if (refundAmount >= 0.001) {
    const refundMemo = `v4call:refund:${callId}:unused-credit`;
    ledgerPayment(callId, 'refund', ESCROW_ACCOUNT, payment.caller, refundAmount, refundMemo, 'pending');
    const r = await sendFromEscrow(payment.caller, refundAmount, refundMemo, currency, callId);
    if (r.success) ledgerPaymentUpdate(callId, 'refund', 'sent', r.txId);
    else           console.error(`[fed-billing] Refund FAILED to @${payment.caller}: ${r.reason}`);
  }

  // 3. Platform fee to us
  if (platformTotal >= 0.001) {
    const feeMemo = `v4call:fee:${callId}:ring+cut`;
    ledgerPayment(callId, 'platform_fee', ESCROW_ACCOUNT, SERVER_HIVE_ACCOUNT, platformTotal, feeMemo, 'pending');
    const r = await sendFromEscrow(SERVER_HIVE_ACCOUNT, platformTotal, feeMemo, currency, callId);
    if (r.success) ledgerPaymentUpdate(callId, 'platform_fee', 'sent', r.txId);
    else           console.error(`[fed-billing] Fee FAILED: ${r.reason}`);
  }

  // Emit receipt to local callee
  const calleeSid = lobbyUsers[payment.callee]?.socketId;
  if (calleeSid) io.to(calleeSid).emit('call-receipt', { ...receipt, perspective: 'callee' });

  // Send receipt for caller back to caller's server
  const peer = federationPeers[callerServer];
  if (peer?.connected) {
    fedSend(peer.ws, {
      type: 'call-receipt-fed',
      callId,
      receipt: { ...receipt, perspective: 'caller' }
    });
  }

  delete activePayments[callId];
}


// ─────────────────────────────────────────────────────────────────────────────
// ── Payment Verification — checks Hive blockchain for a transfer ──────────────
// ─────────────────────────────────────────────────────────────────────────────

async function verifyHivePayment(fromUser, toUser, amount, memo, retries = PAYMENT_VERIFY_RETRIES) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const data = await hivePost({
        jsonrpc: '2.0', method: 'condenser_api.get_account_history',
        params: [toUser, -1, 30], id: 1
      });
      if (!data?.result) {
        console.warn(`[payment] verify attempt ${attempt}/${retries} — no result`);
      } else {
        const cutoff = Date.now() - (5 * 60 * 1000);
        for (const [, op] of data.result) {
          if (op.op[0] !== 'transfer') continue;
          const t = op.op[1];
          if (t.from !== fromUser)    continue;
          if (t.to   !== toUser)      continue;
          if (!t.memo.includes(memo)) continue;
          const txTime   = new Date(op.timestamp + 'Z').getTime();
          if (txTime < cutoff)        continue;
          const txAmount = parseFloat(t.amount.split(' ')[0]);
          if (txAmount >= amount - 0.001) {
            console.log(`[payment] ✓ Verified ${amount} HBD from @${fromUser} (attempt ${attempt})`);
            return true;
          }
        }
        console.warn(`[payment] verify attempt ${attempt}/${retries} — not found yet`);
      }
    } catch(e) {
      console.warn(`[payment] verify attempt ${attempt}/${retries} error:`, e.message);
    }
    if (attempt < retries) await new Promise(r => setTimeout(r, PAYMENT_VERIFY_DELAY_MS));
  }
  console.error(`[payment] ✗ Payment not found after ${retries} attempts`);
  return false;
}


// ─────────────────────────────────────────────────────────────────────────────
// ── Join Tokens (room access, not Hive-Engine tokens) ─────────────────────────
// ─────────────────────────────────────────────────────────────────────────────

const tokens = {}; // short-lived room join tokens: token → { username, pubKey, roomName, expires }

function generateToken(username, pubKey, roomName) {
  const token   = crypto.randomBytes(24).toString('hex');
  tokens[token] = { username, pubKey, roomName, expires: Date.now() + 30000 };
  return token;
}
function consumeToken(token) {
  const t = tokens[token];
  if (!t || Date.now() > t.expires) { delete tokens[token]; return null; }
  delete tokens[token];
  return t;
}
// Clean up expired join tokens
setInterval(() => {
  const now = Date.now();
  for (const k in tokens) if (tokens[k].expires < now) delete tokens[k];
}, 60000);

app.get('/join-token', (req, res) => {
  const t = consumeToken(req.query.token);
  if (!t) return res.status(403).json({ error: 'Invalid or expired token' });
  res.json({ username: t.username, pubKey: t.pubKey, roomName: t.roomName });
});


// ─────────────────────────────────────────────────────────────────────────────
// ── Admin & Debug Endpoints ───────────────────────────────────────────────────
// Set ADMIN_KEY in your .env — endpoints return 403 without it.
// ─────────────────────────────────────────────────────────────────────────────

// GET /admin/ledger?key=ADMIN_KEY&limit=50
// GET /admin/ledger?key=ADMIN_KEY&call_id=CALL_ID
app.get('/admin/ledger', (req, res) => {
  const adminKey = process.env.ADMIN_KEY;
  if (!adminKey || req.query.key !== adminKey) {
    return res.status(403).json({ error: 'Forbidden' });
  }
  const limit  = Math.min(parseInt(req.query.limit) || 50, 500);
  const callId = req.query.call_id;
  try {
    const payments = callId
      ? db.prepare('SELECT * FROM payments WHERE call_id = ? ORDER BY ts DESC').all(callId)
      : db.prepare('SELECT * FROM payments ORDER BY ts DESC LIMIT ?').all(limit);
    const calls = callId
      ? db.prepare('SELECT * FROM calls WHERE call_id = ?').all(callId)
      : db.prepare('SELECT * FROM calls ORDER BY id DESC LIMIT ?').all(limit);
    const summary = db.prepare(`
      SELECT
        COUNT(*) as total_calls,
        SUM(ring_paid) as total_ring,
        SUM(connect_paid) as total_connect,
        SUM(duration_cost) as total_duration,
        SUM(callee_net) as total_paid_out,
        SUM(platform_cut) as total_platform_fee,
        SUM(ring_paid + connect_paid + duration_cost) as total_revenue
      FROM calls WHERE status = 'ended'
    `).get();
    res.json({ summary, calls, payments });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /admin/balance?key=ADMIN_KEY
app.get('/admin/balance', async (req, res) => {
  const adminKey = process.env.ADMIN_KEY;
  if (!adminKey || req.query.key !== adminKey) return res.status(403).json({ error: 'Forbidden' });
  const balance = await getEscrowBalance();
  res.json({ account: ESCROW_ACCOUNT, balance_hbd: balance });
});

// Express needs the urlencoded parser for POST bodies (we use query params
// for admin calls but this keeps things robust if anyone sends a form body).
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

// ── Federation peer admin ─────────────────────────────────────────────────
// GET  /admin/peers?key=ADMIN_KEY
//      Lists every discovered v4call server with verification + approval
//      status, plus the set of currently-connected federation peers.
//
// POST /admin/peers/approve?key=ADMIN_KEY&domain=example.com
//      Adds domain to the approved set, persists, and kicks off an outbound
//      connection if we're the lower-domain tiebreaker initiator.
//
// POST /admin/peers/revoke?key=ADMIN_KEY&domain=example.com
//      Removes domain from approved set, drops any active connection.
//
// POST /admin/peers/rescan?key=ADMIN_KEY
//      Forces an immediate Hive-tag rescan (normally runs every 2h).
// ─────────────────────────────────────────────────────────────────────────
function _requireAdmin(req, res) {
  const adminKey = process.env.ADMIN_KEY;
  if (!adminKey || req.query.key !== adminKey) {
    res.status(403).json({ error: 'Forbidden' });
    return false;
  }
  return true;
}

app.get('/admin/peers', (req, res) => {
  if (!_requireAdmin(req, res)) return;
  const nowMs = Date.now();
  const peers = Object.entries(discoveredPeers).map(([domain, p]) => ({
    domain,
    hive_account:  p.parsed.hive_account,
    escrow:        p.parsed.escrow,
    fee_account:   p.parsed.fee_account,
    federation_ws: p.parsed.federation_ws,
    software:      p.parsed.software || 'unknown',
    protocol:      p.parsed.protocol || 'unknown',
    declared:      p.parsed.declared || null,
    verified:      p.verified,
    verify_reason: p.verify_reason,
    approved:      approvedPeers.has(domain),
    connected:     !!federationPeers[domain]?.connected,
    post_author:   p.post_author,
    post_permlink: p.post_permlink,
    post_created:  p.post_created,
    post_age_h:    p.post_created ? +((nowMs - Date.parse(p.post_created)) / 3600000).toFixed(1) : null,
    last_seen:     p.last_seen
  }));
  const connectedUsers = Object.fromEntries(
    Object.entries(federationPeers)
      .filter(([, p]) => p.connected)
      .map(([d, p]) => [d, p.users ? p.users.size : 0])
  );
  res.json({
    our_domain:        SERVER_DOMAIN,
    discovered:        peers,
    approved_domains:  [...approvedPeers].sort(),
    connected_peers:   connectedUsers
  });
});

app.post('/admin/peers/approve', (req, res) => {
  if (!_requireAdmin(req, res)) return;
  const domain = (req.query.domain || '').toLowerCase().trim();
  if (!domain) return res.status(400).json({ error: 'domain query parameter required' });
  const candidate = discoveredPeers[domain];
  if (!candidate) return res.status(404).json({ error: `domain not found in discovery cache — rescan first`, hint: 'POST /admin/peers/rescan' });
  if (!candidate.verified) return res.status(400).json({ error: `domain is not verified: ${candidate.verify_reason}` });

  approvedPeers.add(domain);
  persistApprovedPeers();
  console.log(`[peers] ✓ Approved @${domain} (via admin endpoint)`);

  // If we're the lower-domain tiebreaker for this peer, open an outbound
  // connection now so the link comes up without waiting for a restart.
  let initiating = false;
  if (candidate.parsed.federation_ws && fedShouldInitiate(domain)) {
    fedConnectPeer(candidate.parsed.federation_ws);
    initiating = true;
  }
  res.json({
    approved:   domain,
    ws:         candidate.parsed.federation_ws,
    initiating,
    note:       initiating
                  ? 'outbound connection starting — watch logs'
                  : 'we are the higher-domain peer; waiting for inbound from ' + domain
  });
});

app.post('/admin/peers/revoke', (req, res) => {
  if (!_requireAdmin(req, res)) return;
  const domain = (req.query.domain || '').toLowerCase().trim();
  if (!domain) return res.status(400).json({ error: 'domain query parameter required' });
  const wasApproved = approvedPeers.delete(domain);
  persistApprovedPeers();
  // Drop any existing connection.
  const peer = federationPeers[domain];
  if (peer?.ws) {
    try { peer.ws.close(1000, 'revoked'); } catch(_) {}
  }
  delete federationPeers[domain];
  console.log(`[peers] ✗ Revoked @${domain} (via admin endpoint)`);
  res.json({ revoked: domain, was_approved: wasApproved });
});

app.post('/admin/peers/rescan', async (req, res) => {
  if (!_requireAdmin(req, res)) return;
  res.json({ scanning: true });
  scanV4CallDirectory().catch(e => console.error('[discovery] manual scan error:', e.message));
});

// GET /admin/discovery-test?key=ADMIN_KEY
// Diagnostic: hits each Hive node directly with the discovery query and
// returns the raw per-node response, then the parsed peer list. Run with
// `docker exec` to see what the running container actually sees.
app.get('/admin/discovery-test', async (req, res) => {
  if (!_requireAdmin(req, res)) return;
  const body = {
    jsonrpc: '2.0',
    method:  'condenser_api.get_discussions_by_created',
    params:  [{ tag: 'v4call-server', limit: 20 }],
    id: 1
  };
  const perNode = [];
  for (const node of HIVE_API_NODES) {
    const t0 = Date.now();
    try {
      const r = await fetch(node, {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify(body),
        signal:  AbortSignal.timeout(8000)
      });
      const ms = Date.now() - t0;
      let parsed = null, raw = null;
      try { parsed = await r.json(); } catch(je) { raw = `non-JSON body: ${je.message}`; }
      perNode.push({
        node,
        http_status: r.status,
        elapsed_ms:  ms,
        ok:          r.ok && parsed && parsed.result !== undefined,
        result_count: parsed && Array.isArray(parsed.result) ? parsed.result.length : null,
        error:       parsed && parsed.error ? parsed.error : null,
        raw_preview: parsed ? JSON.stringify(parsed).slice(0, 400) : raw
      });
    } catch(e) {
      perNode.push({ node, error: e.message, elapsed_ms: Date.now() - t0 });
    }
  }
  // Also report what discovery has stored right now (no rescan triggered).
  const cached = Object.entries(discoveredPeers).map(([domain, d]) => ({
    domain, hive_account: d.parsed?.hive_account, verified: d.verified,
    verify_reason: d.verify_reason, last_seen: d.last_seen
  }));
  res.json({
    hive_nodes_tried: HIVE_API_NODES,
    per_node_response: perNode,
    discovered_peers_cached: cached
  });
});

// GET /debug-state — shows current lobby and room state (no auth, safe info only)
app.get('/debug-state', (req, res) => {
  res.json({
    lobbyUsers: Object.entries(lobbyUsers).map(([username, u]) => ({
      username, socketId: u.socketId, invisible: u.invisible, inCall: u.inCall || null
    })),
    rooms: Object.entries(rooms).map(([name, r]) => ({
      name, creator: r.creator, members: r.members.map(m => m.username),
      allowlist: [...r.allowlist], memberCount: r.members.length
    }))
  });
});

// GET /debug-rates/:username?caller=x&type=voice
// Shows the full parsed rate structure for a user, plus what rates a caller would receive.
// Useful for testing rate posts without making actual calls.
// Clears cache so you always get a fresh fetch.
app.get('/debug-rates/:username', async (req, res) => {
  delete rateCache[req.params.username];
  const rates = await fetchRates(req.params.username);
  if (!rates) {
    return res.json({
      found:   false,
      message: `No v4call-rates post found for @${req.params.username}. ` +
               `Make sure the post exists with title "v4call-rates" and contains a [V4CALL-RATES-V1] or [V4CALL-RATES-V2] block.`
    });
  }
  const caller    = req.query.caller || 'unknown';
  const callType  = req.query.type   || 'voice';
  const applicable = await getRatesForCaller(rates, caller, callType, new Date());

  // Also report what the multi-currency picker would actually show — the
  // single-best `applicable` answer above can hide why the picker UI is empty
  // (e.g. token balance returned 0 from a Hive-Engine hiccup).
  const tokenBalances = [];
  for (const tok of (rates.tokens || [])) {
    // bypass cache so we see fresh balances
    delete tokenBalanceCache[`${caller}:${tok.symbol}`];
    const bal = await getHiveEngineTokenBalance(caller, tok.symbol);
    tokenBalances.push({
      symbol:  tok.symbol,
      caller_balance: bal,
      qualifies: bal > 0,
      has_text:  !!tok.text,
      has_voice: !!tok.voiceRate,
      has_video: !!tok.videoRate
    });
  }

  res.json({
    found:           true,
    version:         rates.version,
    rates,
    applicable,
    picker_diagnostics: {
      caller,
      callType,
      token_balances:  tokenBalances,
      tokens_in_post:  (rates.tokens || []).map(t => t.symbol),
      hbd_lists:       (rates.lists || []).map(l => ({ name: l.name, users: l.users, windowCount: l.windows.length }))
    },
    testedWith: { caller, callType, time: new Date().toISOString() }
  });
});

// GET /rates/:username — client fetches callee rates before showing payment modal
app.get('/rates/:username', async (req, res) => {
  const rates = await fetchRates(req.params.username);
  if (!rates) return res.json({ found: false });
  res.json({ found: true, rates });
});


// ─────────────────────────────────────────────────────────────────────────────
// ── Socket.io — Connection Handler ────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────

io.on('connection', (socket) => {
  console.log('Connected:', socket.id);

  // ── LOBBY ──────────────────────────────────────────────────────────────────

  socket.on('lobby-join', ({ username, pubKey }) => {
    socket._username    = username;
    socket._pubKey      = pubKey;
    socket._invisible   = false;
    socket._room        = null;
    socket._pendingCall = null;

    const prev          = lobbyUsers[username];
    lobbyUsers[username] = { socketId: socket.id, pubKey, invisible: prev ? prev.invisible : false };

    // v0.16.17 — if a pending offline broadcast is scheduled for this user,
    // cancel it: they came back within the grace window, so other users
    // should never see the flap.
    if (pendingOfflineTimers[username]) {
      clearTimeout(pendingOfflineTimers[username]);
      delete pendingOfflineTimers[username];
    }

    socket.emit('lobby-users', lobbySnapshot());
    socket.emit('lobby-rooms', roomsSnapshot());
    socket.emit('lobby-config', {
      serverName: SERVER_NAME,
      serverDomain: SERVER_DOMAIN,
      notice: LOBBY_NOTICE_RESOLVED,
      requirementsText: LOBBY_REQUIREMENTS_RESOLVED
    });
    broadcastLobby();
    broadcastRooms();

    // Announce new user to federated peers.
    if (FEDERATION_ENABLED) fedAnnounceUserOnline(username);

    // ── DM unread summary ──────────────────────────────────────────────────
    const unread = chatGetDmUnread(username);
    if (unread.length > 0) {
      const totalMessages = unread.reduce((sum, r) => sum + r.cnt, 0);
      const fromUsers     = unread.map(r => r.from_user);
      socket.emit('dm-unread-summary', { totalMessages, fromUsers });
    }

    // ── v0.16.9 — Missed/declined/cancelled calls with unrefunded ring fees ──
    const missedCalls = getRefundableMissedCalls(username);
    if (missedCalls.length > 0) {
      socket.emit('missed-calls-summary', { calls: missedCalls });
    }

    // ── v0.16.17 — Pending paid-invite re-delivery ─────────────────────────
    // If someone paid to invite this user while they were offline, the original
    // `room-invite` socket emit never reached them. Without re-delivery, the
    // user only finds out via the inviter's 15-min timeout-refund notice.
    // Scan both local + federated pending paid invites for entries addressed
    // to this user; re-emit room-invite with the same shape used at the
    // original send sites (lines 4083, 5612, etc.).
    try {
      for (const [id, e] of Object.entries(pendingPaidInvites)) {
        if (e.invitee !== username || e.status !== 'pending') continue;
        const r = rooms[e.room];
        const invitees = r ? [...r.allowlist] : [e.invitee, e.inviter];
        socket.emit('room-invite', {
          roomName: e.room, from: e.inviter, invitees,
          paid_invite_id: id, paid_amount: e.paid, paid_currency: e.currency
        });
      }
      for (const [id, e] of Object.entries(pendingFederatedInvites)) {
        if (e.dir !== 'incoming' || !e.paid_invite || e.target_user !== username) continue;
        // Federated paid invites store the payment inside e.payload.payment
        // (see incoming `room-invite` envelope handler around server.js:5701).
        // Mirror the same socket emit shape used at original delivery.
        const pay = e.payload && e.payload.payment ? e.payload.payment : {};
        socket.emit('room-invite', {
          roomName:       e.room,
          from:           e.from_user,
          from_server:    e.from_server,
          invitees:       [e.from_user, e.target_user],
          invite_id:      id,
          paid_invite_id: id,
          paid_amount:    pay.paid || 0,
          paid_currency:  pay.currency || null
        });
      }
    } catch (e) {
      console.warn('[lobby-join] paid-invite re-delivery error:', e.message);
    }

    // ── DM previews (last N per conversation) ──────────────────────────────
    if (DM_PREVIEW_COUNT > 0) {
      const previews = chatGetDmPreviews(username, DM_PREVIEW_COUNT);
      if (previews.length > 0) {
        socket.emit('dm-previews', previews);
      }
    }

    // Update last_seen
    chatUpdateSeen(username);

    console.log(`@${username} entered lobby`);
  });

  socket.on('lobby-invisible', (invisible) => {
    const u = socket._username;
    if (!u || !lobbyUsers[u]) return;
    lobbyUsers[u].invisible = invisible;
    socket._invisible       = invisible;
    broadcastLobby();
    if (FEDERATION_ENABLED) {
      if (invisible) fedAnnounceUserOffline(u);
      else           fedAnnounceUserOnline(u);
    }
  });

  // ── LOBBY CHAT ─────────────────────────────────────────────────────────────

  socket.on('lobby-chat', async ({ message, signature, timestamp }) => {
    const from = socket._username;
    if (!from) return;
    const gate = await checkLobbyPostGate(from);
    if (!gate.allowed) { socket.emit('lobby-post-rejected', { reason: gate.message }); return; }
    io.emit('lobby-chat', { from, message, signature, timestamp });
  });

  // ── LOBBY ENCRYPTED — REMOVED in v0.16.5 ──────────────────────────────────
  // Was a "send encrypted lobby message to selected users" feature that
  // bypassed paid-DM rate enforcement. Removed entirely — for private chat,
  // use rooms (free, allowlist-based) or DMs (paid if recipient demands).
  // Stub stays so stale clients (cached older index.html) get a clear message
  // instead of silent failure.
  socket.on('lobby-encrypted', () => {
    socket.emit('lobby-post-rejected', {
      reason: 'Encrypted lobby messages were removed in v0.16.5. Use a room (free) or DM (paid if rates set) for private chat.'
    });
  });

  // ── LOBBY DM — paid direct message path ───────────────────────────────────
  // Paid DMs: client sends { msgId, textPaid, textMemo } after Keychain approval.
  //           Server verifies on-chain before relaying, then disburses to recipient.
  // Blocked senders are rejected server-side regardless of client state.

  socket.on('lobby-dm', async ({ to, ciphertext, senderCiphertext, signature, timestamp, msgId, textPaid, textMemo, textCurrency }) => {
    const from = socket._username;
    if (!from) return;

    const recipient    = lobbyUsers[to];
    const federatedTo  = recipient ? null : (FEDERATION_ENABLED ? peerForUser(to) : null);
    if (!recipient && !federatedTo) {
      // v0.16.18 — Distinguish "truly offline" from "visible via Nostr but WS
      // federation isn't connected yet". The latter is a transient state after
      // a server restart while WS reconnects; surfacing it as "not online"
      // misleads users into thinking the recipient is unreachable.
      const nostrDomain = nostrSeenDomain(to);
      if (nostrDomain) {
        if (FEDERATION_ENABLED) {
          console.warn(`[text] @${from} → @${to}: visible via Nostr on ${nostrDomain} but WS federation not connected — try again shortly`);
          socket.emit('lobby-dm-error',
            `⏳ @${to} is shown online via Nostr presence (${nostrDomain}), but federation is still reconnecting. Try again in ~30s.`);
        } else {
          // WS federation is disabled in .env — Nostr presence shows the user
          // but there's no transport to deliver. Tell the truth.
          console.warn(`[text] @${from} → @${to}: visible via Nostr on ${nostrDomain} but WS federation is disabled on this server`);
          socket.emit('lobby-dm-error',
            `@${to} is on ${nostrDomain} (visible via Nostr presence), but this server has WS server-to-server federation disabled — cross-server DMs aren't routable. Ask the operator to enable FEDERATION_PEERS in .env.`);
        }
        return;
      }
      const visiblePeers = Object.entries(federationPeers)
        .filter(([, p]) => p.connected)
        .map(([d, p]) => `${d}(${p.users.size})`).join(', ') || 'none';
      console.warn(`[text] @${from} → @${to}: recipient not found locally and not in federated peers [${visiblePeers}]`);
      socket.emit('lobby-dm-error', `@${to} is not online`);
      return;
    }
    console.log(`[text] @${from} → @${to}: routing ${recipient ? 'local' : `federated via ${federatedTo.domain}`}`);

    // Fetch recipient's rates and resolve applicable text rate for this sender
    const calleeRates = await fetchRates(to);
    const applicable  = calleeRates
      ? await getRatesForCaller(calleeRates, from, 'text', new Date())
      : null;

    // Enforce block list server-side — client check is UX only
    if (applicable?.blocked) {
      socket.emit('lobby-dm-error', `🚫 ${applicable.message || 'You have been blocked by this user.'}`);
      return;
    }

    // Enforce platform fee minimum
    if (applicable?.feeRejected) {
      socket.emit('lobby-dm-error', `⚠ ${applicable.message}`);
      return;
    }

    const textRate     = applicable?.flat || 0;
    const cur          = textCurrency || applicable?.currency || 'HBD';
    const calleeEscrow = calleeRates?.escrow || ESCROW_ACCOUNT;

    // For paid federated DMs, the callee's rates-post escrow MUST match the
    // peer server's own ESCROW_ACCOUNT — otherwise the peer holds no key to
    // disburse from it. Fail loudly instead of silently dropping the message.
    if (federatedTo && textRate >= 0.001 && federatedTo.escrow && federatedTo.escrow !== calleeEscrow) {
      socket.emit('lobby-dm-error',
        `⚠ @${to}'s rates post declares escrow @${calleeEscrow}, but ` +
        `${federatedTo.domain} controls @${federatedTo.escrow}. They need to ` +
        `update their rates post to point at @${federatedTo.escrow} (or switch servers) ` +
        `for paid DMs to work across federation.`);
      return;
    }

    if (textRate >= 0.001) {
      // ── Payment required ─────────────────────────────────────────────────
      if (!textPaid || !textMemo || !msgId) {
        socket.emit('lobby-dm-payment-required', {
          to,
          rate:     textRate,
          currency: cur,
          escrow:   calleeEscrow
        });
        return;
      }

      // Verify the transfer against the CALLEE's escrow (their rates post says
      // where to pay) — for federated recipients this is the peer's escrow,
      // not ours. We're only the verifier; disbursement happens on the
      // recipient's server (it owns the escrow key).
      const ok = (cur !== 'HBD' && cur !== 'HIVE')
        ? await verifyHiveEnginePayment(from, calleeEscrow, textPaid, cur, textMemo)
        : await verifyHivePayment(from, calleeEscrow, textPaid, textMemo);
      if (!ok) {
        socket.emit('lobby-dm-error', `Payment not found on blockchain — message not sent. Funds are safe if ${cur} left your account.`);
        return;
      }

      ledgerPayment(msgId, 'text', from, calleeEscrow, textPaid, textMemo, 'verified');

      // For LOCAL recipients we disburse from our own escrow; for FEDERATED
      // recipients we forward the payment info and let their server disburse.
      if (recipient) {
        const platformFee  = applicable.platformFee || PLATFORM_FEE;
        const platformCut  = parseFloat((textPaid * platformFee).toFixed(3));
        const recipientNet = parseFloat((textPaid - platformCut).toFixed(3));

        if (recipientNet >= 0.001) {
          const payoutMemo = `v4call:text-payout:${msgId}`;
          ledgerPayment(msgId, 'text_payout', ESCROW_ACCOUNT, to, recipientNet, payoutMemo, 'pending');
          sendFromEscrow(to, recipientNet, payoutMemo, cur, msgId).then(r => {
            if (r.success) {
              ledgerPaymentUpdate(msgId, 'text_payout', 'sent', r.txId);
              io.to(recipient.socketId).emit('text-payment-received', {
                from, amount: recipientNet, currency: cur, msgId
              });
            } else {
              console.error(`[text] Payout failed to @${to}: ${r.reason}`);
            }
          });
        }

        if (platformCut >= 0.001) {
          const feeMemo = `v4call:text-fee:${msgId}`;
          ledgerPayment(msgId, 'text_fee', ESCROW_ACCOUNT, SERVER_HIVE_ACCOUNT, platformCut, feeMemo, 'pending');
          sendFromEscrow(SERVER_HIVE_ACCOUNT, platformCut, feeMemo, cur, msgId).then(r => {
            if (r.success) ledgerPaymentUpdate(msgId, 'text_fee', 'sent', r.txId);
          });
        }

        console.log(`[text] @${from} → @${to}: paid ${textPaid} ${cur} | net: ${recipientNet} | fee: ${platformCut}`);
      }
    }

    // ── Relay the message ─────────────────────────────────────────────────
    if (recipient) {
      io.to(recipient.socketId).emit('lobby-dm', { from, ciphertext, signature, timestamp, textPaid: textPaid || 0, textCurrency: cur });
      console.log(`[text] @${from} → @${to}: delivered locally (sid ${recipient.socketId.slice(0,6)}…)`);
    } else if (federatedTo) {
      const wsOk = federatedTo.ws && federatedTo.ws.readyState === 1;  // WebSocket.OPEN
      fedSend(federatedTo.ws, {
        type: 'dm',
        from, to, ciphertext, signature, timestamp,
        textPaid:    textPaid    || 0,
        textMemo:    textMemo    || null,
        textCurrency: cur,
        msgId:       msgId       || null,
        fromServer:  SERVER_DOMAIN
      });
      console.log(`[text] @${from} → @${to}@${federatedTo.domain}: fedSend(type=dm) issued (ws.open=${wsOk}, paid=${textPaid || 0} ${cur})`);
    }
    socket.emit('lobby-dm-sent', { to, textPaid: textPaid || 0, textCurrency: cur });

    // ── Store in chat DB (sender's local copy) ────────────────────────────
    // For federated recipients, the peer server stores the recipient's copy.
    chatStoreDm(from, to, ciphertext, senderCiphertext || null, signature, timestamp, textPaid || 0, cur);
  });

  // ── DM HISTORY (on demand) ───────────────────────────────────────────────
  socket.on('dm-history', ({ withUser }, cb) => {
    const username = socket._username;
    if (!username || !withUser) return cb ? cb([]) : null;
    const history = chatGetDmHistory(username, withUser);
    if (cb) cb(history);
    else socket.emit('dm-history', { withUser, messages: history });
  });

  // ── DM MARK READ ─────────────────────────────────────────────────────────
  socket.on('dm-mark-read', () => {
    const username = socket._username;
    if (username) chatUpdateSeen(username);
  });

  // ── FEDERATED CALL ENDED ─────────────────────────────────────────────────
  // Emitted by the callee's client on its HOME-server socket after leaving a
  // federated call that lived on a peer server. Clears the in-call state that
  // was set when we forwarded the incoming-call invite.
  socket.on('federated-call-ended', () => {
    const u = socket._username;
    if (u && lobbyUsers[u]) {
      delete lobbyUsers[u].inCall;
      delete lobbyUsers[u].pendingFederatedCall;
      console.log(`[federation] Cleared call state for @${u}`);
    }
  });

  // ── RATE QUERY ─────────────────────────────────────────────────────────────
  // Client requests callee's applicable rates before initiating a call.
  // Returns: { found, free, rates, escrow } or { found, free: true } or { error }
  // NOTE: getRatesForCaller is async (may check Hive-Engine token balances)

  socket.on('get-rates', async ({ callee, callType }, cb) => {
    const caller = socket._username;
    if (!caller || !callee || !callType) return cb({ error: 'Missing parameters' });

    const rates = await fetchRates(callee);
    if (!rates) {
      // No rates post found → free call
      return cb({ found: false, free: true });
    }

    const applicable = await getRatesForCaller(rates, caller, callType, new Date());

    if (applicable?.blocked) {
      return cb({ found: true, blocked: true, message: applicable.message });
    }

    if (applicable?.feeRejected) {
      return cb({ found: true, feeRejected: true, message: applicable.message });
    }

    if (!applicable) {
      return cb({ found: true, free: true });
    }

    cb({ found: true, free: false, rates: applicable, escrow: rates.escrow || ESCROW_ACCOUNT });
  });

  // ── ALL RATE OPTIONS (for payment picker) ──────────────────────────────────
  // Returns every qualifying payment option so the client can show a choice.
  socket.on('get-all-rates', async ({ callee, callType }, cb) => {
    const caller = socket._username;
    if (!caller || !callee || !callType) return cb({ error: 'Missing parameters' });

    const rates = await fetchRates(callee);
    if (!rates) return cb({ found: false, free: true });

    const result = await computePaymentOptions(rates, caller, callType, new Date());
    if (result.feeRejected) {
      return cb({ found: true, feeRejected: true, message: result.message });
    }
    if (result.blocked) {
      return cb({ found: true, blocked: true, message: result.message });
    }
    if (result.options.length === 0) {
      return cb({ found: true, free: true, belowFloor: result.belowFloor || false });
    }
    result.options[0]._recommended = true;
    cb({ found: true, free: false, options: result.options, escrow: rates.escrow || ESCROW_ACCOUNT, belowFloor: result.belowFloor || false });
  });

  // ── PAYMENT VERIFICATION ───────────────────────────────────────────────────

  socket.on('verify-ring-payment', async ({ callId, callee, amount, memo, currency, escrow }, cb) => {
    const caller = socket._username;
    if (!caller) return cb({ verified: false, reason: 'Not authenticated' });
    const cur = currency || 'HBD';
    // Verify against the actual escrow the caller paid — for federated callees
    // this is the peer's escrow account, not our local one.
    const targetEscrow = escrow || ESCROW_ACCOUNT;

    console.log(`[payment] Verifying ring: @${caller} → @${targetEscrow} ${amount} ${cur} (memo: ${memo})`);
    const ok = (cur !== 'HBD' && cur !== 'HIVE')
      ? await verifyHiveEnginePayment(caller, targetEscrow, amount, cur, memo)
      : await verifyHivePayment(caller, targetEscrow, amount, memo);

    if (ok) {
      if (!activePayments[callId]) activePayments[callId] = {};
      activePayments[callId].ringPaid    = amount;
      activePayments[callId].ringMemo    = memo;
      activePayments[callId].caller      = caller;
      activePayments[callId].callee      = callee;
      activePayments[callId].currency    = cur;
      activePayments[callId].escrow      = targetEscrow;

      // Fetch and store rate info for billing at call end
      const rates = await fetchRates(callee);
      if (rates) {
        const applicable = await getRatesForCaller(rates, caller, 'voice', new Date());
        if (applicable && !applicable.blocked && !applicable.feeRejected) {
          activePayments[callId].ratePerHour = applicable.rate       || 0;
          activePayments[callId].minDeposit  = applicable.minDeposit || 0;
          activePayments[callId].platformFee = applicable.platformFee || PLATFORM_FEE;
        }
      }
      console.log(`[payment] Ring fee verified for ${callId} — rate: ${activePayments[callId].ratePerHour} ${cur}/hr`);

      // If callee is on a federated peer, forward verified payment so peer can settle at end.
      if (FEDERATION_ENABLED) {
        const peer = peerForUser(callee);
        if (peer) {
          activePayments[callId].calleeServer = peer.domain;
          fedSend(peer.ws, {
            type: 'payment-verified',
            paymentType: 'ring',
            callId, from: caller, to: callee, amount, currency: cur, memo,
            ratePerHour: activePayments[callId].ratePerHour,
            platformFee: activePayments[callId].platformFee,
            callType: 'voice',
            callerServer: SERVER_DOMAIN
          });
        }
      }
    }

    cb({ verified: ok, reason: ok ? null : 'Payment not found on blockchain' });
  });

  socket.on('verify-deposit-payment', async ({ callId, callee, totalAmount, depositAmount, connectAmount, memo, currency, escrow }, cb) => {
    const caller = socket._username;
    if (!caller) return cb && cb({ verified: false, reason: 'Not authenticated' });
    const cur = currency || 'HBD';
    const targetEscrow = escrow || ESCROW_ACCOUNT;

    console.log(`[payment] Verifying deposit: @${caller} → @${targetEscrow} ${totalAmount} ${cur} (memo: ${memo})`);
    const ok = (cur !== 'HBD' && cur !== 'HIVE')
      ? await verifyHiveEnginePayment(caller, targetEscrow, totalAmount, cur, memo)
      : await verifyHivePayment(caller, targetEscrow, totalAmount, memo);

    if (ok) {
      if (!activePayments[callId]) activePayments[callId] = {};
      activePayments[callId].depositPaid     = depositAmount;
      activePayments[callId].connectPaid     = connectAmount || 0;
      activePayments[callId].creditRemaining = depositAmount;
      activePayments[callId].caller          = caller;
      activePayments[callId].callee          = callee;
      activePayments[callId].currency        = cur;
      activePayments[callId].escrow          = targetEscrow;
      console.log(`[payment] ✓ Deposit verified ${callId}: total=${totalAmount} connect=${connectAmount} deposit=${depositAmount} ${cur}`);
      ledgerPayment(callId, 'deposit', caller, targetEscrow, totalAmount, memo, 'verified');

      if (FEDERATION_ENABLED) {
        const peer = peerForUser(callee);
        if (peer) {
          activePayments[callId].calleeServer = peer.domain;
          fedSend(peer.ws, {
            type: 'payment-verified',
            paymentType: 'deposit',
            callId, from: caller, to: callee,
            amount: totalAmount, depositAmount, connectAmount: connectAmount || 0,
            currency: cur, memo,
            callerServer: SERVER_DOMAIN
          });
        }
      }
    }

    if (cb) cb({ verified: ok, reason: ok ? null : 'Payment not found on blockchain' });
  });

  socket.on('verify-topup-payment', async ({ callId, amount, memo, currency, escrow }, cb) => {
    const caller = socket._username;
    if (!caller) return cb && cb({ verified: false });
    const cur = currency || 'HBD';
    const targetEscrow = escrow || activePayments[callId]?.escrow || ESCROW_ACCOUNT;

    const ok = (cur !== 'HBD' && cur !== 'HIVE')
      ? await verifyHiveEnginePayment(caller, targetEscrow, amount, cur, memo)
      : await verifyHivePayment(caller, targetEscrow, amount, memo);

    if (ok && activePayments[callId]) {
      activePayments[callId].depositPaid     = (activePayments[callId].depositPaid     || 0) + amount;
      activePayments[callId].creditRemaining = (activePayments[callId].creditRemaining || 0) + amount;
      const minLeft = activePayments[callId].creditRemaining / ((activePayments[callId].ratePerHour || 0) / 60);
      const room    = socket._room;
      if (room) io.to(room).emit('credit-topup', {
        amount, creditRemaining: activePayments[callId].creditRemaining, minutesLeft: minLeft
      });
      ledgerPayment(callId, 'topup', caller, targetEscrow, amount, memo, 'verified');
      console.log(`[payment] Top-up ${amount} ${cur} for call ${callId}`);

      if (FEDERATION_ENABLED && activePayments[callId].calleeServer) {
        const peer = federationPeers[activePayments[callId].calleeServer];
        if (peer?.connected) {
          fedSend(peer.ws, {
            type: 'payment-verified',
            paymentType: 'topup',
            callId, from: caller, to: activePayments[callId].callee,
            amount, currency: cur, memo,
            callerServer: SERVER_DOMAIN
          });
        }
      }
    }
    if (cb) cb({ verified: ok });
  });

  socket.on('verify-connect-payment', async ({ callId, callee, amount, memo }) => {
    const caller = socket._username;
    if (!caller) return;
    if (!activePayments[callId]) activePayments[callId] = {};
    const ok = await verifyHivePayment(caller, ESCROW_ACCOUNT, amount, memo);
    if (ok) {
      activePayments[callId].connectPaid = amount;
      console.log(`[payment] Connect fee verified for ${callId}: ${amount} HBD`);
    }
  });

  // ── v0.16.9 — REFUND MISSED/DECLINED/CANCELLED RING FEE ────────────────────
  // The callee clicks "refund" on a missed-call popup row. We send the ring fee
  // back from our escrow to the caller, in the original currency. Same code
  // path for local + federated: the funds are on the callee's home server
  // (this server) regardless of where the caller is on Hive.
  socket.on('refund-ring-fee', async ({ callId }, cb) => {
    const callee = socket._username;
    if (!callee) return cb && cb({ success: false, reason: 'Not authenticated' });
    if (!callId || typeof callId !== 'string') return cb && cb({ success: false, reason: 'Missing callId' });

    // Look up the call + ring payment + check no refund already in flight
    let row;
    try {
      row = db.prepare(`
        SELECT
          c.call_id, c.caller, c.callee, c.status,
          p.amount   AS ring_paid,
          p.currency AS ring_currency
        FROM calls c
        JOIN payments p
          ON p.call_id = c.call_id
         AND p.type    = 'ring'
         AND p.status IN ('verified', 'sent')
        WHERE c.call_id = ?
          AND c.callee  = ?
          AND c.status  IN ('missed', 'declined', 'cancelled')
          AND p.amount  > 0
          AND NOT EXISTS (
            SELECT 1 FROM payments r
            WHERE r.call_id = c.call_id
              AND r.type IN ('refund', 'ring_refund')
              AND r.status IN ('pending', 'sent')
          )
      `).get(callId, callee);
    } catch(e) {
      console.error('[refund-ring-fee] lookup failed:', e.message);
      return cb && cb({ success: false, reason: 'Internal error' });
    }
    if (!row) {
      return cb && cb({ success: false, reason: 'Refund not available — already refunded, no ring fee, or not yours.' });
    }

    const refundMemo = `v4call:ring-refund:${callId}:user-action`;
    const cur        = row.ring_currency || 'HBD';
    ledgerPayment(callId, 'ring_refund', ESCROW_ACCOUNT, row.caller, row.ring_paid, refundMemo, 'pending', null, cur);

    try {
      const r = await sendFromEscrow(row.caller, row.ring_paid, refundMemo, cur, callId);
      if (r.success) {
        ledgerPaymentUpdate(callId, 'ring_refund', 'sent', r.txId);
        console.log(`[refund-ring-fee] @${callee} refunded ${row.ring_paid} ${cur} to @${row.caller} (${callId})`);
        // Notify the caller if they're online on this server.
        const callerSid = lobbyUsers[row.caller]?.socketId;
        if (callerSid) io.to(callerSid).emit('ring-fee-refunded', {
          callId, by: callee, amount: row.ring_paid, currency: cur
        });
        return cb && cb({ success: true, amount: row.ring_paid, currency: cur, txId: r.txId });
      } else {
        ledgerPaymentUpdate(callId, 'ring_refund', 'failed', null);
        console.error(`[refund-ring-fee] Disbursement failed: ${r.reason}`);
        return cb && cb({ success: false, reason: r.reason || 'Disbursement failed' });
      }
    } catch(e) {
      ledgerPaymentUpdate(callId, 'ring_refund', 'failed', null);
      console.error('[refund-ring-fee] exception:', e.message);
      return cb && cb({ success: false, reason: e.message });
    }
  });

  // ── CALL END (explicit hang-up) ────────────────────────────────────────────

  socket.on('call-end', async ({ callId }) => {
    const username = socket._username;
    const room     = socket._room;
    if (!room || !rooms[room]) return;
    if (rooms[room].isCall) {
      // Real 1:1 call — `peer-hung-up` cascades to the other party so they
      // also leave (which is the correct behaviour for a 2-person call).
      socket.to(room).emit('peer-hung-up', { by: username });
      const cid = rooms[room].callId || callId;
      if (cid) await processCallEnd(cid, room, io, lobbyUsers);
      console.log(`📵 @${username} ended 1:1 call in #${room}`);
    } else {
      // Defensive: someone hit `call-end` in a multi-party room. Treat it as a
      // normal leave for just this user — do NOT kick everyone. Mirrors the
      // `leave-room` handler's cleanup. Without this gate the original bug
      // (one user leaves → everyone kicked to lobby) reappears.
      rooms[room].members = rooms[room].members.filter(u => u.socketId !== socket.id);
      socket.to(room).emit('user-left', socket.id);
      if (lobbyUsers[username]) delete lobbyUsers[username].inRoom;
      socket.leave(room);
      socket._room = null;
      if (rooms[room].members.length === 0) {
        if (rooms[room]._capTimer)  clearTimeout(rooms[room]._capTimer);
        if (rooms[room]._warnTimer) clearTimeout(rooms[room]._warnTimer);
        delete rooms[room];
        chatDeleteRoom(room);
        console.log(`Room #${room} closed (last member left via call-end-on-non-call)`);
      } else {
        console.log(`@${username} call-end on multi-party room #${room} — treated as leave-room (${rooms[room].members.length} remaining)`);
      }
      broadcastRooms();
    }
  });

  // ── DIRECT CALL — initiate ─────────────────────────────────────────────────

  socket.on('call-user', ({ callee, callType, ringFeePaid, callId }) => {
    const caller = socket._username;
    if (!caller) {
      socket.emit('call-failed', { reason: 'Session not found — please refresh.' });
      return;
    }

    const getSocketId = (username) => lobbyUsers[username]?.socketId;

    // Free-ring cooldown (paid rings bypass this — ring fee is the skin in the game)
    if (!ringFeePaid || ringFeePaid === 0) {
      const cool = checkCallCooldown(caller, callee);
      if (!cool.allowed) {
        const waitSec = Math.ceil(cool.waitMs / 1000);
        socket.emit('call-failed', { reason: `Please wait ${waitSec}s before calling @${callee} again.` });
        return;
      }
    }

    // Determine where the callee lives — local socket or federated peer.
    const federatedCallee = lobbyUsers[callee] ? null : (FEDERATION_ENABLED ? peerForUser(callee) : null);

    // Escrow-mismatch guard for paid federated calls — same reason as for DMs
    // (see comment in lobby-dm handler). We only bother checking when a ring
    // fee was paid; free calls don't care about disbursement.
    if (federatedCallee && ringFeePaid > 0) {
      // Use an async lookup inline — we already fetched rates client-side for
      // the payment flow, but double-check here before routing the invite.
      fetchRates(callee).then(r => {
        const declaredEscrow = r?.escrow || null;
        if (declaredEscrow && federatedCallee.escrow && declaredEscrow !== federatedCallee.escrow) {
          socket.emit('call-failed', {
            reason: `⚠ @${callee}'s rates post declares escrow @${declaredEscrow}, ` +
              `but ${federatedCallee.domain} controls @${federatedCallee.escrow}. ` +
              `Funds paid cannot be disbursed. Contact @${callee} to update their rates.`
          });
        }
      }).catch(() => {});
      // Let the rest of the flow proceed — worst case the clean error reaches
      // the user before the callee's server rejects the on-chain re-verify.
    }

    // Callee must be online (locally or on a federated peer)
    if (!lobbyUsers[callee] && !federatedCallee) {
      // v0.16.19 — Server-side parity with the client call-precheck. The client
      // already guards against this, but a stale browser or race could still get
      // here. Distinguish "truly offline" from "visible via Nostr but no WS
      // transport" so the caller gets an accurate reason. Funds are protected by
      // the existing refund branch regardless.
      const nostrDomain = nostrSeenDomain(String(callee).toLowerCase());
      const offlineReason = nostrDomain
        ? (FEDERATION_ENABLED
            ? `@${callee} is shown online via Nostr presence on ${nostrDomain}, but WS federation to that server isn't connected right now — calls can't route. Try again in ~30s.`
            : `@${callee} is on ${nostrDomain} (visible via Nostr presence), but this server has WS federation disabled — there's no transport to ring them. Operator needs to enable FEDERATION_PEERS in .env.`)
        : `@${callee} is not online.`;

      if (ringFeePaid && ringFeePaid > 0 && callId) {
        // Callee unreachable between rate check and ring — refund ring fee
        const ringCurrency = activePayments[callId]?.currency || 'HBD';
        const refundMemo = `v4call:refund:${callId}:unreachable`;
        ledgerPayment(callId, 'refund', ESCROW_ACCOUNT, caller, ringFeePaid, refundMemo, 'pending', null, ringCurrency);
        sendFromEscrow(caller, ringFeePaid, refundMemo, ringCurrency, callId).then(r => {
          const msg = r.success
            ? `${offlineReason} Ring fee of ${ringFeePaid.toFixed(3)} ${ringCurrency} refunded.`
            : `${offlineReason} Refund of ${ringFeePaid.toFixed(3)} ${ringCurrency} pending — contact support.`;
          socket.emit('call-failed', { reason: msg, refunded: r.success });
          if (r.success) ledgerPaymentUpdate(callId, 'refund', 'sent', r.txId);
        });
      } else {
        socket.emit('call-failed', { reason: offlineReason });
      }
      return;
    }

    if (lobbyUsers[callee]?.inCall) {
      socket.emit('call-failed', { reason: `@${callee} is already in a call.` });
      return;
    }
    if (lobbyUsers[caller]?.inCall) {
      socket.emit('call-failed', { reason: 'You are already in a call.' });
      return;
    }

    const roomName        = `call__${caller}__${callee}__${Date.now()}`;
    const effectiveCallId = callId || roomName;

    rooms[roomName] = {
      creator:           caller,
      allowlist:         new Set([caller, callee]),
      // v0.16 Part B.6 — 1:1 call rooms must initialise the same fields the
      // generic join/leave/info/ban code expects on multi-party rooms; without
      // these the join handler crashed (banlist undefined, paidInvitees undefined,
      // [...r.banlist] crash for the creator's room-info path, etc.).
      banlist:           new Set(),
      tokenGate:         null,
      banlistVisibility: 'admin',
      paidInvitees:      new Map(),
      spotlight:         null,
      members:           [],
      createdAt:         new Date(),
      isCall:            true,
      callType:          callType || 'voice',
      callId:            effectiveCallId,
      federated:         federatedCallee ? { calleeServer: federatedCallee.domain } : null
    };

    ledgerCallCreate(effectiveCallId, caller, callee, callType || 'voice');
    ledgerCallUpdate(effectiveCallId, { status: 'ringing' });
    if (ringFeePaid > 0) {
      // v0.16.9 — record currency from activePayments (set by verify-ring-payment)
      // so the missed-call popup can show the actual currency, not always 'HBD'.
      const ringCurrency = activePayments[effectiveCallId]?.currency || 'HBD';
      ledgerPayment(effectiveCallId, 'ring', caller, ESCROW_ACCOUNT, ringFeePaid,
        `v4call:ring:${effectiveCallId}:${callee}`, 'verified', null, ringCurrency);
    }

    if (lobbyUsers[caller]) lobbyUsers[caller].inCall = roomName;
    if (lobbyUsers[callee]) lobbyUsers[callee].inCall = roomName;
    socket._pendingCall = { roomName, callee, federatedTo: federatedCallee?.domain || null };

    console.log(`📞 @${caller} → @${callee}${federatedCallee ? '@' + federatedCallee.domain : ''} (${callType}) room: ${roomName}`);

    if (federatedCallee) {
      // Ring the callee through their home server.
      fedSend(federatedCallee.ws, {
        type:         'call-invite',
        caller,
        callee,
        callType:     callType || 'voice',
        roomName,
        callerPubKey: socket._pubKey,
        callerServer: SERVER_DOMAIN,
        ringFeePaid:  ringFeePaid || 0
      });
    } else {
      const calleeSid = getSocketId(callee);
      if (calleeSid) {
        io.to(calleeSid).emit('incoming-call', {
          caller, callerPubKey: socket._pubKey,
          roomName, callType: callType || 'voice', ringFeePaid: ringFeePaid || 0
        });
      } else {
        socket.emit('call-failed', { reason: `@${callee} is online but unreachable. Ask them to refresh.` });
        delete rooms[roomName];
        if (lobbyUsers[caller]) delete lobbyUsers[caller].inCall;
        if (lobbyUsers[callee]) delete lobbyUsers[callee].inCall;
        return;
      }
    }

    socket.emit('call-ringing', { callee, roomName });

    // 30-second ring timeout → missed call
    const timer = setTimeout(() => {
      if (rooms[roomName] && rooms[roomName].members.length === 0) {
        const wasFederated = rooms[roomName].federated;
        // v0.16.9 — persist missed status so the callee sees a refund popup later.
        ledgerCallUpdate(effectiveCallId, {
          status: 'missed',
          end_reason: 'timeout',
          ended_at: new Date().toISOString()
        });
        delete rooms[roomName];
        if (lobbyUsers[caller]) delete lobbyUsers[caller].inCall;
        if (lobbyUsers[callee]) delete lobbyUsers[callee].inCall;
        socket._pendingCall = null;
        socket.emit('call-missed', { callee, roomName });
        if (wasFederated && federatedCallee) {
          fedSend(federatedCallee.ws, { type: 'call-missed', caller, callee, roomName });
        } else {
          const calleeSidNow = getSocketId(callee);
          if (calleeSidNow) io.to(calleeSidNow).emit('call-missed', { caller, roomName });
        }
        broadcastRooms();
        console.log(`⏰ Timed out: @${caller} → @${callee}`);
      }
    }, 30000);

    rooms[roomName]._callTimer = timer;
    broadcastRooms();
  });

  socket.on('call-response', ({ roomName, accepted }) => {
    const callee = socket._username;

    // Federated incoming call — we don't host the room; relay response to caller's server.
    const fedPending = callee && lobbyUsers[callee]?.pendingFederatedCall;
    if (fedPending && fedPending.roomName === roomName) {
      const peer = federationPeers[fedPending.callerServer];
      if (peer?.connected) {
        fedSend(peer.ws, {
          type: accepted ? 'call-response' : 'call-declined',
          caller: fedPending.caller,
          callee,
          accepted: !!accepted,
          roomName
        });
      }
      if (!accepted) {
        if (lobbyUsers[callee]) delete lobbyUsers[callee].inCall;
        delete lobbyUsers[callee].pendingFederatedCall;
      } else {
        socket.emit('call-accepted', {
          caller: fedPending.caller,
          roomName,
          callerServer: fedPending.callerServer
        });
      }
      return;
    }

    const room   = rooms[roomName];
    if (!room) return;
    const caller = room.creator;
    if (room._callTimer) { clearTimeout(room._callTimer); delete room._callTimer; }

    if (accepted) {
      const callerSid = lobbyUsers[caller]?.socketId;
      if (callerSid) io.to(callerSid).emit('call-accepted', { callee, roomName });
      socket.emit('call-accepted', { caller, roomName });
      const now = Date.now();
      if (activePayments[room.callId]) activePayments[room.callId].startTime = now;
      ledgerCallUpdate(room.callId, { connected_at: new Date(now).toISOString(), status: 'connected' });
      startCreditBurn(room.callId, roomName);

      // Duration cap — disconnect when max call length reached
      const capMs   = MAX_CALL_DURATION_MIN * 60 * 1000;
      const warnMs  = Math.max(0, (MAX_CALL_DURATION_MIN - 5) * 60 * 1000);
      const capTimer = setTimeout(async () => {
        if (rooms[roomName]) {
          io.to(roomName).emit('call-cap-reached', { maxMinutes: MAX_CALL_DURATION_MIN });
          await processCallEnd(room.callId, roomName, io, lobbyUsers, 'cap_reached');
        }
      }, capMs);
      const warnTimer = setTimeout(() => {
        if (rooms[roomName]) io.to(roomName).emit('call-cap-warning', { minutesLeft: 5 });
      }, warnMs);
      if (rooms[roomName]) { rooms[roomName]._capTimer = capTimer; rooms[roomName]._warnTimer = warnTimer; }
      console.log(`✅ @${callee} accepted call from @${caller} — ${room.callId}`);
    } else {
      const cid = room.callId;
      delete rooms[roomName];
      if (lobbyUsers[caller]) delete lobbyUsers[caller].inCall;
      if (lobbyUsers[callee]) delete lobbyUsers[callee].inCall;
      const callerSid = lobbyUsers[caller]?.socketId;
      if (callerSid) io.to(callerSid).emit('call-declined', { callee, roomName });
      if (cid) ledgerCallUpdate(cid, { status: 'declined', end_reason: 'declined', ended_at: new Date().toISOString() });
      broadcastRooms();
      console.log(`❌ @${callee} declined call from @${caller}`);
    }
  });

  socket.on('call-cancel', ({ roomName }) => {
    const caller = socket._username;
    const room   = rooms[roomName];
    if (!room) return;
    const callee = [...room.allowlist].find(u => u !== caller);
    const federated = room.federated;
    if (room._callTimer) { clearTimeout(room._callTimer); delete room._callTimer; }
    // v0.16.9 — persist cancelled status so the callee sees a refund popup
    // later for the unspent ring fee.
    if (room.callId) {
      ledgerCallUpdate(room.callId, {
        status: 'cancelled',
        end_reason: 'cancelled_by_caller',
        ended_at: new Date().toISOString()
      });
    }
    delete rooms[roomName];
    if (lobbyUsers[caller]) delete lobbyUsers[caller].inCall;
    if (callee && lobbyUsers[callee]) delete lobbyUsers[callee].inCall;
    if (federated) {
      const peer = federationPeers[federated.calleeServer];
      if (peer?.connected) fedSend(peer.ws, { type: 'call-cancelled', caller, callee, roomName });
    } else {
      const calleeSid = lobbyUsers[callee]?.socketId;
      if (callee && calleeSid) io.to(calleeSid).emit('call-cancelled', { caller, roomName });
    }
    socket._pendingCall = null;
    broadcastRooms();
    console.log(`🚫 @${caller} cancelled call to @${callee}`);
  });

  // ── ROOM CREATION ──────────────────────────────────────────────────────────

  socket.on('room-check', (roomName, cb) => { cb({ available: !rooms[roomName] }); });

  socket.on('room-create', async ({ roomName, invitees = [], tokenGateSymbol = '', tokenGateAmount = 0, banlistVisibility = 'admin' }) => {
    const creator = socket._username;
    if (!creator) return;
    if (rooms[roomName]) {
      socket.emit('room-create-error', `Room "${roomName}" already exists.`);
      return;
    }

    // Resolve each invitee to either a local username, a federated 'user@server'
    // form, or skip if unresolvable. Two paths are honoured for federated:
    //   1. Caller passed canonical 'user@server.com' explicitly
    //   2. Caller passed bare 'user' but no local lobby user with that name
    //      exists AND a federation peer has them online (v0.13+ presence)
    // Path 2 covers the lobby user-picker UX, where federated users in the list
    // get added to lobbySelected as bare names. Without this fallback, the
    // bare name fails the local lookup and silently drops the invite.
    const resolved = []; // each: { local: bool, user, server, canonical, peer? }
    for (const raw of invitees) {
      if (raw === creator) continue;
      const handle = parseFederatedHandle(raw);
      if (!handle) continue;

      if (handle.server && handle.server !== SERVER_DOMAIN.toLowerCase()) {
        if (!approvedPeers.has(handle.server))     continue;
        const peer = federationPeers[handle.server];
        if (!peer || !peer.connected)              continue;
        if (!peerSupportsV04(handle.server))       continue;
        resolved.push({ local: false, user: handle.user, server: handle.server, canonical: `${handle.user}@${handle.server}`, peer });
        continue;
      }

      // Local-typed name — but maybe a federated user via lobby-picker fallback.
      if (lobbyUsers[handle.user]) {
        resolved.push({ local: true, user: handle.user, canonical: handle.user });
        continue;
      }
      const fedPeer = peerForUser(handle.user);
      if (fedPeer && peerSupportsV04(fedPeer.domain)) {
        resolved.push({ local: false, user: handle.user, server: fedPeer.domain, canonical: `${handle.user}@${fedPeer.domain}`, peer: fedPeer });
      }
      // else: unresolvable, drop silently (matches existing behaviour for unknown locals).
    }

    // v0.16.10 — gate LOCAL invitees on invite rate. Paid invitees are deferred:
    // the room is created without them on the allowlist; the admin gets a notice
    // listing who needs paying, and uses the allowlist panel (which has the
    // payment flow) after entering the room.
    // v0.16.13 — same gate now applies to FEDERATED invitees (closes the bypass
    // where ticking @user@peer.com in the lobby user-picker and clicking Create
    // Room would send a free room-invite to the peer regardless of the user's
    // invite rate). Federated paid invitees are deferred the same way locals
    // are; admin uses the allowlist panel afterwards which routes through the
    // federated paid-invite flow (modal → Keychain → recipient validates).
    const deferred  = []; // invitees with a paid invite rate (or blocked / fee-rejected)
    const okLocals  = []; // locals that can be invited free
    const okFededs  = []; // federated invitees that can be invited free
    for (const r of resolved) {
      const inv = await getInviteOptions(r.user, creator);
      // The deferral "user" label includes the server suffix for federated so
      // the admin's in-room notice + allowlist pre-fill route through the
      // canonical @user@server form.
      const dispUser = r.local ? r.user : `${r.user}@${r.server}`;
      if (inv.blocked || inv.feeRejected) {
        deferred.push({ user: dispUser, reason: inv.message || 'cannot invite' });
        continue;
      }
      if (inv.options.length === 0) {
        if (r.local) okLocals.push(r);
        else         okFededs.push(r);
        continue;
      }
      // Paid — escrow-mismatch sanity check for federated (same guard as
      // allowlist-add fed path); if it fails the admin can't pay them via
      // allowlist either, so it's a hard reject (not deferred).
      if (!r.local) {
        const peerEscrow = r.peer && r.peer.escrow;
        if (peerEscrow && inv.escrow !== peerEscrow) {
          deferred.push({ user: dispUser, reason: `escrow mismatch — rates declare @${inv.escrow}, peer controls @${peerEscrow}` });
          continue;
        }
      }
      deferred.push({ user: dispUser, reason: `paid invite (${inv.options.map(o => `${o.flat} ${o.currency}`).join(' or ')})` });
    }
    const finalLocalsCanonical = okLocals.map(r => r.canonical);
    const fedCanonical         = okFededs.map(r => r.canonical);

    const allowlist = new Set([creator, ...finalLocalsCanonical, ...fedCanonical]);
    const sym = (tokenGateSymbol || '').trim().toUpperCase();
    const amt = parseFloat(tokenGateAmount) || 0;
    const tokenGate  = (sym && amt > 0) ? { symbol: sym, amount: amt } : null;
    const visibility = (banlistVisibility === 'all') ? 'all' : 'admin';
    rooms[roomName] = {
      creator,
      allowlist,
      banlist:           new Set(),
      tokenGate,
      banlistVisibility: visibility,
      paidInvitees:      new Map(), // v0.17 forward-compat — never populated in v0.14
      members:           [],
      spotlight:         null,      // v0.15 — admin-broadcast spotlight target (username or null)
      createdAt:         new Date()
    };

    // Deferred paid invitees are surfaced via the room-created event below so
    // the client can render a single prominent notice INSIDE the new room
    // (lobby-info from before was too quiet — easy to miss when you've just
    // entered the room). v0.16.12 — UX polish.

    const allInviteCanonical = [creator, ...finalLocalsCanonical, ...fedCanonical];
    for (const r of okLocals) {
      const lu = lobbyUsers[r.user];
      if (lu) io.to(lu.socketId).emit('room-invite', { roomName, from: creator, invitees: allInviteCanonical });
    }
    for (const r of okFededs) {
      // Federated — fire room-invite over the federation socket. Same envelope
      // shape and pendingFederatedInvites bookkeeping as the allowlist-add path.
      // v0.16.13 — only reached for FREE federated invitees; paid ones are in
      // the `deferred` list and have to be invited via the allowlist panel.
      const inviteId = crypto.randomBytes(12).toString('hex');
      pendingFederatedInvites[inviteId] = {
        dir:           'outgoing',
        room:          roomName,
        from_user:     creator,
        target_user:   r.user,
        target_server: r.server,
        created_at:    Date.now()
      };
      fedSend(r.peer.ws, {
        type:          'room-invite',
        invite_id:     inviteId,
        from_user:     creator,
        to_user:       r.user,
        room_name:     roomName,
        source_server: SERVER_DOMAIN,
        payload:       {}
      });
      console.log(`[federation] → room-invite @${creator} → @${r.user}@${r.server} #${roomName} (id ${inviteId})`);
      io.to(socket.id).emit('lobby-info', { text: `📨 Invite sent to @${r.user}@${r.server}.` });
    }

    broadcastRooms();
    socket.emit('room-created', {
      roomName,
      invitees: [...finalLocalsCanonical, ...fedCanonical],
      deferred // v0.16.12 — each entry: { user, reason } so the client can
               // render a clear "click to invite + pay" notice in the room.
    });
    console.log(`@${creator} created #${roomName}${tokenGate ? ` [tokenGate: ${tokenGate.amount} ${tokenGate.symbol}]` : ''}${visibility === 'all' ? ' [banlist: public]' : ''}${deferred.length ? ` [deferred paid: ${deferred.map(d=>d.user).join(',')}]` : ''}`);
  });

  socket.on('request-join-token', ({ roomName }, cb) => {
    const username = socket._username;
    const pubKey   = socket._pubKey;
    if (!username || !pubKey) return cb({ error: 'Not authenticated' });
    cb({ token: generateToken(username, pubKey, roomName) });
  });

  // ── ROOM JOINING ───────────────────────────────────────────────────────────

  socket.on('join', async ({ room, username, pubKey, homeServer }) => {
    // v0.16 Part B — federated joiners send homeServer (their own server's
    // domain). The host server validates against the allowlist's canonical
    // form ('user@server.com'). Token-gate (chain check) uses the bare
    // username — Hive identity is server-agnostic.
    const myDomain = SERVER_DOMAIN.toLowerCase();
    const fedHome  = (typeof homeServer === 'string' && homeServer.trim().toLowerCase()) || null;
    const isFed    = !!fedHome && fedHome !== myDomain;
    const canonicalUser = isFed ? `${username}@${fedHome}` : username;

    // 1. Banlist — check both canonical form (federated) and bare (local /
    // legacy ban entries). Banlist always wins. 1:1 call rooms are created
    // without a banlist field (see the call-invite handler), so guard with `b &&`
    // — without this guard, joining a 1:1 call room crashes the whole server.
    if (rooms[room]) {
      const b = rooms[room].banlist;
      if (b && (b.has(canonicalUser) || (isFed && b.has(username)))) {
        socket.emit('join-rejected', { room, reason: 'You are banned from this room.' });
        return;
      }
    }

    // 2. Auto-create room on first join (matches v0.13 behaviour).
    if (!rooms[room]) {
      rooms[room] = {
        creator:           username,
        allowlist:         new Set([username]),
        banlist:           new Set(),
        tokenGate:         null,
        banlistVisibility: 'admin',
        paidInvitees:      new Map(),
        members:           [],
        spotlight:         null,
        createdAt:         new Date()
      };
    }
    const r = rooms[room];

    // 3. Authorisation: allowlist OR (v0.17 forward-compat) paidInvitees OR
    // tokenGate-with-balance. Federated joiners try the canonical form first;
    // for legacy 1:1 call rooms (created with `new Set([caller, callee])`,
    // both BARE names — see the call-invite handler) we fall back to the bare
    // username so a federated callee can still join the call room hosted on
    // the caller's server. v0.16 federated rooms always have canonical entries
    // so the canonical match wins; the bare-fallback is only for 1:1 calls.
    let joinedVia = null;
    if      (r.allowlist.has(canonicalUser))            joinedVia = 'allowlist';
    else if (isFed && r.allowlist.has(username))        joinedVia = 'allowlist'; // 1:1 call legacy path
    else if (r.paidInvitees.has(canonicalUser))         joinedVia = 'paid';      // v0.17 hook
    else if (isFed && r.paidInvitees.has(username))     joinedVia = 'paid';      // v0.17 hook (1:1)
    else if (r.tokenGate) {
      const bal = await getHiveEngineTokenBalance(username, r.tokenGate.symbol);
      if (bal >= r.tokenGate.amount)                    joinedVia = 'token';
    }
    if (!joinedVia) {
      const need = r.tokenGate
        ? `allowlist or ${r.tokenGate.amount} ${r.tokenGate.symbol}`
        : 'allowlist';
      socket.emit('join-rejected', { room, reason: `Not authorised — needs ${need}.` });
      return;
    }

    socket.join(room);
    socket._room = room;
    socket._homeServer = isFed ? fedHome : null; // remembered for disconnect cleanup
    // Drop any stale entry for this username before pushing the fresh one.
    // Socket.io reconnects mint a new socketId for the same client; without
    // this dedup, the server's member count drifts upward over reconnects.
    // Use canonical form for the dedup key so a local @user can't accidentally
    // displace a federated @user@peer.com.
    const memberKey = m => (m.homeServer ? `${m.username}@${m.homeServer}` : m.username);
    const staleBefore = r.members.length;
    // v0.16.17 — collect stale socketIds before filtering so we can notify them
    // they were displaced (popout into a new tab leaves the first tab's UI
    // thinking it's still in the room — server has already dropped its
    // membership). The `displaced` event lets that tab reset to lobby cleanly.
    const staleMembers = r.members.filter(u => memberKey(u) === canonicalUser);
    r.members = r.members.filter(u => memberKey(u) !== canonicalUser);
    if (r.members.length !== staleBefore) {
      console.log(`[join] dropped ${staleBefore - r.members.length} stale member entry/entries for ${canonicalUser} in #${room}`);
      for (const stale of staleMembers) {
        if (stale.socketId === socket.id) continue;  // don't notify self (this socket is the new arrival)
        io.to(stale.socketId).emit('displaced', {
          room,
          reason: 'You joined this room from another tab or device.'
        });
      }
    }
    r.members.push({ socketId: socket.id, username, pubKey, joinedVia, homeServer: isFed ? fedHome : null });
    if (!isFed && lobbyUsers[username]) lobbyUsers[username].inRoom = room;

    const everyone = r.members.map(u => ({
      socketId: u.socketId, username: u.username, pubKey: u.pubKey,
      joinedVia: u.joinedVia, homeServer: u.homeServer || null
    }));
    socket.emit('room-users', everyone);
    socket.emit('room-info', {
      creator:           r.creator,
      allowlist:         [...r.allowlist],
      tokenGate:         r.tokenGate || null,
      banlist:           (canonicalUser === r.creator || r.banlistVisibility === 'all') ? [...r.banlist] : null,
      banlistVisibility: r.banlistVisibility,
      spotlight:         r.spotlight || null
    });

    // Send room history (broadcasts + messages encrypted to this user)
    const history = chatGetRoomHistory(room, username);
    if (history.length > 0) {
      socket.emit('room-history', history);
    }
    // Send attachment envelope history (sender + per_recipient match only —
    // late joiners don't see attachments addressed to others sent pre-join).
    const attHistory = chatGetRoomAttachments(room, username);
    if (attHistory.length > 0) {
      socket.emit('room-attachments-history', attHistory);
    }

    socket.to(room).emit('user-joined', {
      socketId: socket.id, username, pubKey, joinedVia,
      homeServer: isFed ? fedHome : null
    });
    broadcastRooms();
    console.log(`@${canonicalUser} joined #${room} via ${joinedVia} (${r.members.length} members)`);
  });

  // ── ALLOWLIST MANAGEMENT ───────────────────────────────────────────────────

  socket.on('allowlist-add', async ({ room, username: targetUser, payment }) => {
    const r = rooms[room];
    if (!r || r.creator !== socket._username) return;

    const handle = parseFederatedHandle(targetUser);
    if (!handle) {
      socket.emit('allowlist-error', { room, reason: 'Invalid username — expected @user or @user@server.com.' });
      return;
    }

    if (handle.server) {
      // Federated invite (v0.16 / fed v0.4): target lives on another server.
      if (handle.server === SERVER_DOMAIN.toLowerCase()) {
        // User typed our own domain — treat as a local invite.
        handle.server = null;
      }
    }

    if (handle.server) {
      // ── Federated path ─────────────────────────────────────────────────────
      if (!approvedPeers.has(handle.server)) {
        socket.emit('allowlist-error', { room, reason: `${handle.server} is not an approved federation peer. Approve it at /admin-peers.html first.` });
        return;
      }
      const peer = federationPeers[handle.server];
      if (!peer || !peer.connected) {
        socket.emit('allowlist-error', { room, reason: `Not currently connected to ${handle.server}. Wait for federation to reconnect, then retry.` });
        return;
      }
      if (!peerSupportsV04(handle.server)) {
        socket.emit('allowlist-error', { room, reason: `${handle.server} is on an older federation protocol — cross-server room invites need v0.4. Ask the operator to update.` });
        return;
      }

      // v0.16.11 — federated invite-rate gate. Inviter-holds-funds model:
      // payment goes to OUR escrow, we keep it until accept (disburse net to
      // invitee cross-chain + fee to OUR operator) or decline/timeout (refund
      // inviter from our escrow). Recipient server re-validates and re-verifies
      // on-chain before delivering the popup (design rule #15).
      const fedInviter = socket._username;
      const fedInvitee = handle.user;
      const canonical  = `${fedInvitee}@${handle.server}`;

      // Re-invite of someone already on the allowlist is a no-op (no double-charge).
      if (r.allowlist.has(canonical)) {
        socket.emit('allowlist-info', { room, message: `@${fedInvitee}@${handle.server} is already invited.` });
        return;
      }

      const fedInv = await getInviteOptions(fedInvitee, fedInviter);
      if (fedInv.blocked) {
        socket.emit('allowlist-error', { room, reason: fedInv.message || `@${fedInvitee} has blocked you.` });
        return;
      }
      if (fedInv.feeRejected) {
        socket.emit('allowlist-error', { room, reason: fedInv.message || `@${fedInvitee}'s platform fee is below this server's minimum.` });
        return;
      }

      const isPaid = fedInv.options.length > 0;

      // Paid invite — guard against escrow mismatch on the RECIPIENT side. The
      // peer hello announced the peer's ESCROW account; if the invitee's rates
      // post points elsewhere, the recipient server can't validate consistently
      // and we'd be paying into our own escrow knowing the recipient's policy
      // check will reject (and we'd refund). Fail loudly before Keychain pops.
      if (isPaid) {
        const peerEscrow = peer.escrow || null;
        if (peerEscrow && fedInv.escrow !== peerEscrow) {
          socket.emit('allowlist-error', { room, reason: `@${fedInvitee}'s rates post declares escrow @${fedInv.escrow}, but ${handle.server} controls @${peerEscrow}. They need to update their rates post (or switch home server) for paid invites to clear.` });
          return;
        }
      }

      // v0.16.14 — payment-provided wins over isPaid. The previous flow's
      // `if (!isPaid) free; return;` short-circuited the picker even when the
      // inviter HAD paid (e.g. via the fee_required cycle from the recipient,
      // where source-side rates are cached-stale or just plain disagree).
      // Now: if payment is provided, route through the paid path regardless;
      // the recipient does the authoritative rate validation.
      const hasFedPayment = !!(payment && payment.currency && payment.paid > 0 && payment.memo && payment.inviteId);

      if (!isPaid && !hasFedPayment) {
        // Free federated invite — original behaviour. Recipient will re-check
        // (v0.16.14) and bounce with fee_required if their rates disagree.
        r.allowlist.add(canonical);
        const inviteId = crypto.randomBytes(12).toString('hex');
        pendingFederatedInvites[inviteId] = {
          dir:           'outgoing',
          room,
          from_user:     fedInviter,
          target_user:   fedInvitee,
          target_server: handle.server,
          created_at:    Date.now()
        };
        fedSend(peer.ws, {
          type:          'room-invite',
          invite_id:     inviteId,
          from_user:     fedInviter,
          to_user:       fedInvitee,
          room_name:     room,
          source_server: SERVER_DOMAIN,
          payload:       {}
        });
        console.log(`[federation] → room-invite @${fedInviter} → @${fedInvitee}@${handle.server} #${room} (id ${inviteId})`);
        socket.emit('allowlist-info', { room, message: `📨 Invite sent to @${fedInvitee}@${handle.server}.` });
        emitRoomInfoToMembers(r, room);
        broadcastRooms();
        return;
      }

      // Paid federated invite — emit picker if no payment yet (only when
      // source's own rates say it's paid; the fee_required cycle is handled
      // by the source-side room-response handler which emits the picker
      // directly using the recipient-provided options).
      if (isPaid && !hasFedPayment) {
        const inviteId = crypto.randomBytes(12).toString('hex');
        socket.emit('invite-payment-required', {
          room,
          invitee:  canonical, // show @user@server in modal so admin knows it's federated
          inviteId,
          escrow:   ESCROW_ACCOUNT, // OUR escrow — inviter-holds-funds model
          options:  fedInv.options.map(o => ({
            currency: o.currency,
            flat:     o.flat,
            balance:  o.balance ?? null,
            listName: o.listName ?? null
          })),
          belowFloor: !!fedInv.belowFloor,
          federated:  true,
          peerServer: handle.server
        });
        return;
      }

      // Payment was provided. Validate against source's options when source
      // also thinks it's paid (UX guard — catches obvious mistakes). When
      // source thinks free but payment was provided (post-fee_required
      // cycle), skip source-side rate validation — recipient is the
      // authoritative gate and the on-chain verify below catches the rest.
      if (isPaid) {
        const fedOpt = fedInv.options.find(o => o.currency === payment.currency);
        if (!fedOpt) {
          socket.emit('allowlist-error', { room, reason: `Currency ${payment.currency} is not an accepted invite rate for @${fedInvitee}.` });
          return;
        }
        if (payment.paid + 1e-9 < fedOpt.flat) {
          socket.emit('allowlist-error', { room, reason: `Underpaid: invite rate is ${fedOpt.flat} ${fedOpt.currency}, you sent ${payment.paid}.` });
          return;
        }
      }
      if (payment.memo !== `v4call:invite:${payment.inviteId}`) {
        socket.emit('allowlist-error', { room, reason: 'Bad payment memo — must match the invite ID issued by the server.' });
        return;
      }
      if (pendingPaidInvites[payment.inviteId] || pendingFederatedInvites[payment.inviteId]) {
        socket.emit('allowlist-error', { room, reason: 'This invite has already been processed.' });
        return;
      }

      // Verify on-chain — payment is to OUR escrow (inviter-holds-funds).
      const fedCur = payment.currency;
      const fedOk = (fedCur !== 'HBD' && fedCur !== 'HIVE')
        ? await verifyHiveEnginePayment(fedInviter, ESCROW_ACCOUNT, payment.paid, fedCur, payment.memo)
        : await verifyHivePayment(fedInviter, ESCROW_ACCOUNT, payment.paid, payment.memo);
      if (!fedOk) {
        socket.emit('allowlist-error', { room, reason: `Payment not found on blockchain. If ${fedCur} left your account, it can be recovered manually — note the memo: ${payment.memo}` });
        return;
      }

      // Record both: paid-invite (for refund/disburse) and federated-invite
      // (for peer-state tracking). Same id ties them together so the room-
      // response handler can find the payment to settle.
      pendingPaidInvites[payment.inviteId] = {
        room,
        inviter:    fedInviter,
        invitee:    fedInvitee,             // bare username — Hive account is server-agnostic
        invitee_server: handle.server,
        currency:   fedCur,
        paid:       payment.paid,
        memo:       payment.memo,
        status:     'pending',
        created_at: Date.now(),
        type:       'allowlist',
        federated:  true
      };
      pendingFederatedInvites[payment.inviteId] = {
        dir:           'outgoing',
        room,
        from_user:     fedInviter,
        target_user:   fedInvitee,
        target_server: handle.server,
        created_at:    Date.now(),
        paid_invite:   true
      };
      ledgerPayment(payment.inviteId, 'invite', fedInviter, ESCROW_ACCOUNT, payment.paid, payment.memo, 'verified', null, fedCur);

      r.allowlist.add(canonical);
      fedSend(peer.ws, {
        type:          'room-invite',
        invite_id:     payment.inviteId,
        from_user:     fedInviter,
        to_user:       fedInvitee,
        room_name:     room,
        source_server: SERVER_DOMAIN,
        payload: {
          payment: {
            currency:      fedCur,
            paid:          payment.paid,
            memo:          payment.memo,
            source_escrow: ESCROW_ACCOUNT // recipient verifies the on-chain payment was here
          }
        }
      });
      console.log(`[paid-invite][fed] ${payment.inviteId} — @${fedInviter} paid ${payment.paid} ${fedCur} to invite @${fedInvitee}@${handle.server} → #${room}`);
      socket.emit('allowlist-info', { room, message: `📨 Paid invite (${payment.paid.toFixed(3)} ${fedCur}) sent to @${fedInvitee}@${handle.server}.` });
      emitRoomInfoToMembers(r, room);
      broadcastRooms();
      return;
    }

    // ── Local path ───────────────────────────────────────────────────────────
    // v0.16.10 — invite-rate gate. If the invitee's rates post sets an invite
    // fee, the inviter must pay it. Free invites work as before.
    const inviter = socket._username;
    const invitee = handle.user;

    // Re-invite of someone already on the allowlist is a no-op (avoid charging
    // twice for the same allowlist entry).
    if (r.allowlist.has(invitee)) {
      const lu = lobbyUsers[invitee];
      if (lu) io.to(lu.socketId).emit('room-invite', { roomName: room, from: inviter, invitees: [...r.allowlist] });
      emitRoomInfoToMembers(r, room);
      broadcastRooms();
      return;
    }

    const inv = await getInviteOptions(invitee, inviter);
    if (inv.blocked) {
      socket.emit('allowlist-error', { room, reason: inv.message || `@${invitee} has blocked you.` });
      return;
    }
    if (inv.feeRejected) {
      socket.emit('allowlist-error', { room, reason: inv.message || `@${invitee}'s platform fee is below this server's minimum.` });
      return;
    }

    if (inv.options.length === 0) {
      // Free invite (no invite rate set, or matches a free list/token).
      r.allowlist.add(invitee);
      const lu = lobbyUsers[invitee];
      if (lu) io.to(lu.socketId).emit('room-invite', { roomName: room, from: inviter, invitees: [...r.allowlist] });
      emitRoomInfoToMembers(r, room);
      broadcastRooms();
      return;
    }

    // Paid invite — guard against escrow mismatch. For a LOCAL invitee, their
    // rates-post escrow must be the one this server controls; otherwise we
    // could verify a payment to an escrow we can't disburse from (orphan funds).
    if (inv.escrow !== ESCROW_ACCOUNT) {
      socket.emit('allowlist-error', { room, reason: `@${invitee}'s rates post declares escrow @${inv.escrow}, but this server controls @${ESCROW_ACCOUNT}. Ask them to update their rates post (or switch home server) before charging invite fees.` });
      return;
    }

    // Paid invite. If the client didn't include a payment, send the picker.
    if (!payment || !payment.currency || !(payment.paid > 0) || !payment.memo || !payment.inviteId) {
      const inviteId = crypto.randomBytes(12).toString('hex');
      socket.emit('invite-payment-required', {
        room,
        invitee,
        inviteId,
        escrow: inv.escrow,
        options: inv.options.map(o => ({
          currency: o.currency,
          flat:     o.flat,
          balance:  o.balance ?? null,
          listName: o.listName ?? null
        })),
        belowFloor: !!inv.belowFloor
      });
      return;
    }

    // Validate the payment matches one of the picker's offered options.
    const opt = inv.options.find(o => o.currency === payment.currency);
    if (!opt) {
      socket.emit('allowlist-error', { room, reason: `Currency ${payment.currency} is not an accepted invite rate for @${invitee}.` });
      return;
    }
    if (payment.paid + 1e-9 < opt.flat) {
      socket.emit('allowlist-error', { room, reason: `Underpaid: invite rate is ${opt.flat} ${opt.currency}, you sent ${payment.paid}.` });
      return;
    }

    // Memo must reference the inviteId the client got from invite-payment-required.
    if (payment.memo !== `v4call:invite:${payment.inviteId}`) {
      socket.emit('allowlist-error', { room, reason: 'Bad payment memo — must match the invite ID issued by the server.' });
      return;
    }

    // Reject reuse of the same inviteId (one payment, one invite).
    if (pendingPaidInvites[payment.inviteId]) {
      socket.emit('allowlist-error', { room, reason: 'This invite has already been processed.' });
      return;
    }

    // Verify on-chain. Funds went to OUR escrow (recipient is local on this server).
    const cur = payment.currency;
    const ok = (cur !== 'HBD' && cur !== 'HIVE')
      ? await verifyHiveEnginePayment(inviter, ESCROW_ACCOUNT, payment.paid, cur, payment.memo)
      : await verifyHivePayment(inviter, ESCROW_ACCOUNT, payment.paid, payment.memo);
    if (!ok) {
      socket.emit('allowlist-error', { room, reason: `Payment not found on blockchain. If ${cur} left your account, it can be recovered manually — note the memo: ${payment.memo}` });
      return;
    }

    // Record + add to allowlist + emit invite (now carrying paidInviteId so the
    // recipient's accept/decline can be tracked).
    pendingPaidInvites[payment.inviteId] = {
      room,
      inviter,
      invitee,
      currency:   cur,
      paid:       payment.paid,
      memo:       payment.memo,
      status:     'pending',
      created_at: Date.now(),
      type:       'allowlist'
    };
    ledgerPayment(payment.inviteId, 'invite', inviter, ESCROW_ACCOUNT, payment.paid, payment.memo, 'verified', null, cur);

    r.allowlist.add(invitee);
    const lu = lobbyUsers[invitee];
    if (lu) io.to(lu.socketId).emit('room-invite', {
      roomName: room, from: inviter, invitees: [...r.allowlist],
      paid_invite_id: payment.inviteId,
      paid_amount:    payment.paid,
      paid_currency:  cur
    });
    socket.emit('allowlist-info', { room, message: `📨 Paid invite (${payment.paid.toFixed(3)} ${cur}) sent to @${invitee}.` });
    emitRoomInfoToMembers(r, room);
    broadcastRooms();
    console.log(`[paid-invite] ${payment.inviteId} — @${inviter} paid ${payment.paid} ${cur} to invite @${invitee} → #${room}`);
  });

  // v0.16 / fed v0.4 — local user's accept/decline of a federated room invite.
  // The receiving server forwards the response back over the federation socket
  // to the source server so it can clean up its outgoing-pending entry.
  socket.on('room-invite-respond', ({ invite_id, response }) => {
    const entry = pendingFederatedInvites[invite_id];
    if (!entry || entry.dir !== 'incoming') return;
    if (entry.target_user !== socket._username) {
      console.warn(`[room-invite-respond] @${socket._username} responded to invite ${invite_id} owned by @${entry.target_user} — dropped`);
      return;
    }
    const peer = federationPeers[entry.from_server];
    if (!peer || !peer.connected) {
      console.warn(`[room-invite-respond] Lost connection to ${entry.from_server} — response not relayed for ${invite_id}`);
      socket.emit('lobby-info', { text: `⚠ Lost connection to ${entry.from_server}; your response wasn't delivered.` });
      delete pendingFederatedInvites[invite_id];
      return;
    }
    const finalResponse = (response === 'accepted') ? 'accepted' : 'declined';
    fedSend(peer.ws, { type: 'room-response', invite_id, response: finalResponse });
    delete pendingFederatedInvites[invite_id];
    console.log(`[federation] → room-response ${finalResponse} for invite_id ${invite_id} → ${entry.from_server}`);
  });

  // v0.16.10 — local paid-invite accept/decline. Only the actual invitee can
  // resolve their own invite; on accept we disburse, on decline we refund.
  // Same-server only — federated paid invites are a v0.16.11 follow-up.
  socket.on('paid-invite-respond', ({ paid_invite_id, response }) => {
    const e = pendingPaidInvites[paid_invite_id];
    if (!e || e.status !== 'pending') return;
    if (e.invitee !== socket._username) {
      console.warn(`[paid-invite] @${socket._username} tried to respond to invite ${paid_invite_id} owned by @${e.invitee} — dropped`);
      return;
    }
    if (response === 'accepted') {
      disbursePaidInvite(paid_invite_id);
    } else {
      // Decline → refund the inviter the gross paid amount and remove the
      // invitee from the room's allowlist (so they can't sneak in later off
      // the same paid slot).
      const r = rooms[e.room];
      if (r) r.allowlist.delete(e.invitee);
      refundPaidInvite(paid_invite_id, 'declined');
      if (r) emitRoomInfoToMembers(r, e.room);
      broadcastRooms();
    }
  });

  socket.on('allowlist-remove', ({ room, username: targetUser }) => {
    const r = rooms[room];
    if (!r || r.creator !== socket._username) return;
    const target = (targetUser || '').trim().toLowerCase().replace(/^@/, '');
    if (!target || target === r.creator) return;
    r.allowlist.delete(target);
    // Auto-kick the matching member — same canonical-or-bare match as room-ban.
    const member = r.members.find(m => {
      const canonical = m.homeServer ? `${m.username}@${m.homeServer}` : m.username;
      return canonical === target || m.username === target;
    });
    if (member) io.to(member.socketId).emit('kicked', { room, reason: 'You were removed from this room.' });
    emitRoomInfoToMembers(r, room);
    broadcastRooms();
  });

  // ── BANLIST (v0.14) — admin-only; overrides allowlist + tokenGate ─────────

  socket.on('room-ban', ({ room, username, reason }) => {
    const r = rooms[room];
    if (!r || r.creator !== socket._username) return;
    const target = (username || '').trim().toLowerCase().replace(/^@/, '');
    if (!target || target === r.creator) return; // can't ban self
    r.banlist.add(target);
    // Auto-kick if currently in the room. Match against canonical form for
    // federated members (so banning 'noblemage@hive-book.com' kicks the right
    // user) and also bare username (so banning '@noblemage' still works for
    // local members + as a fallback for federated when there's no name clash).
    const memberIdx = r.members.findIndex(m => {
      const canonical = m.homeServer ? `${m.username}@${m.homeServer}` : m.username;
      return canonical === target || m.username === target;
    });
    if (memberIdx >= 0) {
      const targetSocketId = r.members[memberIdx].socketId;
      io.to(targetSocketId).emit('kicked', { room, reason: `You have been banned from #${room}.` });
      r.members.splice(memberIdx, 1);
      io.to(room).emit('user-left', targetSocketId);
      clearSpotlightIfMember(r, room, target);
    }
    emitRoomInfoToMembers(r, room);
    broadcastRooms();
    console.log(`[ban] @${target} banned from #${room} by @${socket._username}${reason ? ' — ' + reason : ''}`);
  });

  socket.on('room-unban', ({ room, username }) => {
    const r = rooms[room];
    if (!r || r.creator !== socket._username) return;
    const target = (username || '').trim().toLowerCase().replace(/^@/, '');
    if (!target) return;
    r.banlist.delete(target);
    emitRoomInfoToMembers(r, room);
    console.log(`[ban] @${target} unbanned from #${room} by @${socket._username}`);
  });

  // ── END ROOM FOR EVERYONE (v0.14 admin feature) ───────────────────────────
  // Admin closes the whole room — every member receives a `kicked` event with
  // a clear reason and the room (plus its stored messages) is deleted.

  socket.on('room-end-all', () => {
    const room = socket._room;
    const r    = rooms[room];
    if (!r || r.creator !== socket._username) return;
    for (const m of [...r.members]) {
      io.to(m.socketId).emit('kicked', { room, reason: `Room #${room} was ended by the admin (@${r.creator}).` });
    }
    if (r._capTimer)  clearTimeout(r._capTimer);
    if (r._warnTimer) clearTimeout(r._warnTimer);
    r.members = [];
    delete rooms[room];
    chatDeleteRoom(room);

    // v0.16.17 — cancel any pending invites for this room. Without this, an
    // invitee who hadn't responded yet could accept a stale invite afterwards
    // (creating a fresh room as new admin), and paid invites would sit in
    // escrow until the 15-min TTL sweep refunded them.
    try {
      // Local paid invites — immediate refund + invitee popup close.
      for (const [id, e] of Object.entries(pendingPaidInvites)) {
        if (e.room !== room || e.status !== 'pending') continue;
        const inviteeSid = lobbyUsers[e.invitee]?.socketId;
        if (inviteeSid) io.to(inviteeSid).emit('invite-cancelled', { room, reason: 'room_ended' });
        if (typeof refundPaidInvite === 'function') {
          refundPaidInvite(id, 'room_ended');
        } else {
          delete pendingPaidInvites[id];
        }
      }
      // Federated invites (may or may not be paid). Tell invitee server to
      // clean up + refund-if-paid; close invitee popup if they're local on us.
      for (const [id, e] of Object.entries(pendingFederatedInvites)) {
        if (e.room !== room) continue;
        const inviteeSid = e.target_user ? lobbyUsers[e.target_user]?.socketId : null;
        if (inviteeSid) io.to(inviteeSid).emit('invite-cancelled', { room, reason: 'room_ended' });
        if (e.target_server) {
          const peer = federationPeers[e.target_server];
          if (peer?.connected) {
            try {
              fedSend(peer.ws, {
                type: 'room-response',
                invite_id: id, response: 'declined', reason: 'room_ended',
                source_server: SERVER_DOMAIN
              });
            } catch (_) { /* peer drop → their own TTL sweep handles cleanup */ }
          }
        }
        delete pendingFederatedInvites[id];
      }
    } catch (e) {
      console.warn('[room-end-all] pending-invite cleanup error:', e.message);
    }

    broadcastRooms();
    console.log(`Room #${room} ended-for-all by admin @${socket._username}`);
  });

  // ── v0.15 SPOTLIGHT BROADCAST (admin-only) ────────────────────────────────
  // Admin sets a room-wide spotlight target. Stored as username (stable across
  // socket reconnects) and re-resolved to socketId at broadcast time. Soft
  // override on the client side — users with a local pin keep it and see a
  // "↺ Follow room spotlight" affordance instead of being yanked.

  socket.on('room-spotlight-set', ({ room, target }) => {
    const r = rooms[room];
    if (!r || r.creator !== socket._username) return;
    let normalised = null;
    let targetSocketId = null;
    if (target) {
      normalised = String(target).trim().toLowerCase().replace(/^@/, '');
      const member = r.members.find(m => m.username === normalised);
      if (!member) return;
      targetSocketId = member.socketId;
    }
    r.spotlight = normalised;
    io.to(room).emit('room-spotlight-changed', { target: normalised, targetSocketId });
    console.log(`[spotlight] @${socket._username} set #${room} spotlight → ${normalised || '(cleared)'}`);
  });

  // ── v0.15 ADMIN ROLE TRANSFER ─────────────────────────────────────────────
  // Current admin hands off to another current member. Target is forced onto
  // the allowlist (so they can re-join if they later leave + come back).

  socket.on('room-transfer-admin', ({ room, username }) => {
    const r = rooms[room];
    if (!r || r.creator !== socket._username) return;
    const target = String(username || '').trim().toLowerCase().replace(/^@/, '');
    if (!target) return;
    if (target === r.creator) return;
    if (!r.members.some(m => m.username === target)) {
      socket.emit('room-admin-transfer-failed', { reason: `@${target} must be a current room member to receive admin.` });
      return;
    }
    const previous = r.creator;
    r.creator = target;
    r.allowlist.add(target);
    emitRoomInfoToMembers(r, room);
    broadcastRooms();
    console.log(`[admin] #${room} transferred from @${previous} → @${target}`);
  });

  // ── ROOM EXPORT (.v4room) — v0.14.5 ───────────────────────────────────────
  // Any current member can export a snapshot of the room's metadata + every
  // stored message (ciphertext only — we never had plaintext). The file is
  // self-contained and can be re-imported on the same or a different v4call
  // server. Recipients of the imported messages can decrypt only the ones
  // addressed to their key, exactly like the original room.

  socket.on('room-export', ({ roomName }, cb) => {
    const username = socket._username;
    if (typeof cb !== 'function') return;
    if (!username)               return cb({ error: 'Not authenticated' });
    if (!roomName)               return cb({ error: 'Room name required' });
    const r = rooms[roomName];
    if (!r)                                                      return cb({ error: `Room #${roomName} does not exist on this server` });
    if (!r.members.some(m => m.username === username))           return cb({ error: 'Only current room members can export' });

    const messages = chatGetRoomMessagesAll(roomName);
    const file = {
      file_type:      'v4room',
      format_version: 1,
      source_server:  SERVER_DOMAIN,
      exported_at:    new Date().toISOString(),
      exported_by:    username,
      room: {
        name:              roomName,
        creator:           r.creator,
        allowlist:         [...r.allowlist],
        tokenGate:         r.tokenGate || null,
        banlist:           [...r.banlist],
        banlistVisibility: r.banlistVisibility,
        created_at:        (r.createdAt instanceof Date) ? r.createdAt.toISOString() : r.createdAt
      },
      messages: messages.map(m => ({
        from_user:    m.from_user,
        to_user:      m.to_user,
        ciphertext:   m.ciphertext,
        signature:    m.signature,
        timestamp:    m.timestamp,
        created_at:   m.created_at,
        is_broadcast: m.is_broadcast
      }))
    };
    console.log(`[export] @${username} exported #${roomName} (${messages.length} messages)`);
    cb({ ok: true, file });
  });

  // ── ROOM IMPORT (.v4room) — v0.14.5 ───────────────────────────────────────
  // Any logged-in user can import. The destination room is owned by the
  // *importer* (not the original creator), preserves the file's allowlist
  // (importer auto-added), and preserves token-gate / banlist / visibility.
  // Server only does structural validation — cryptographic signature checks
  // stay client-side at decryption time.

  socket.on('room-import', ({ file, roomName: requestedName }, cb) => {
    const username = socket._username;
    if (typeof cb !== 'function') return;
    if (!username)                                                                                return cb({ error: 'Not authenticated' });
    if (!file || typeof file !== 'object')                                                        return cb({ error: 'Missing file' });
    if (file.file_type !== 'v4room')                                                              return cb({ error: 'Not a v4room file (file_type mismatch)' });
    if (file.format_version !== 1)                                                                return cb({ error: `Unsupported format_version ${file.format_version}` });
    if (!file.room || typeof file.room !== 'object')                                              return cb({ error: 'Malformed file — missing room metadata' });
    if (!Array.isArray(file.messages))                                                            return cb({ error: 'Malformed file — missing messages array' });

    const newName = String(requestedName || file.room.name || '').trim();
    if (!newName)                                                                                 return cb({ error: 'Invalid room name' });
    if (newName.length > 64)                                                                      return cb({ error: 'Room name too long (max 64 chars)' });
    if (!/^[a-zA-Z0-9_-]+$/.test(newName))                                                        return cb({ error: 'Room name may only contain a–z, 0–9, _ and -' });
    if (rooms[newName])                                                                           return cb({ error: `Room #${newName} already exists on this server`, collision: true });

    // Build allowlist (importer auto-added)
    const allowSrc = Array.isArray(file.room.allowlist) ? file.room.allowlist : [];
    const allowlist = new Set(allowSrc.filter(u => typeof u === 'string').map(u => u.toLowerCase()));
    allowlist.add(username);

    // Banlist
    const banSrc = Array.isArray(file.room.banlist) ? file.room.banlist : [];
    const banlist = new Set(banSrc.filter(u => typeof u === 'string').map(u => u.toLowerCase()));

    // Token gate
    let tokenGate = null;
    if (file.room.tokenGate && typeof file.room.tokenGate === 'object') {
      const sym = String(file.room.tokenGate.symbol || '').trim().toUpperCase();
      const amt = parseFloat(file.room.tokenGate.amount) || 0;
      if (sym && amt > 0) tokenGate = { symbol: sym, amount: amt };
    }

    rooms[newName] = {
      creator:           username, // importer becomes the new admin
      allowlist,
      banlist,
      tokenGate,
      banlistVisibility: file.room.banlistVisibility === 'all' ? 'all' : 'admin',
      paidInvitees:      new Map(),
      members:           [],
      createdAt:         new Date()
    };

    // Restore messages — defensive, skip rows that don't look right
    let restored = 0, skipped = 0;
    for (const m of file.messages) {
      if (!m || typeof m.ciphertext !== 'string' || typeof m.from_user !== 'string') {
        skipped++; continue;
      }
      try {
        chatStoreRoomMsg(
          newName,
          m.from_user,
          (typeof m.to_user === 'string') ? m.to_user : '',
          m.ciphertext,
          (typeof m.signature === 'string') ? m.signature : '',
          (typeof m.timestamp === 'string') ? m.timestamp : '',
          m.is_broadcast ? 1 : 0
        );
        restored++;
      } catch(e) {
        skipped++;
      }
    }

    broadcastRooms();
    console.log(`[import] @${username} imported #${newName} from ${file.source_server || 'unknown'} (${restored} messages restored, ${skipped} skipped, original creator: @${file.room.creator || 'unknown'})`);
    cb({ ok: true, roomName: newName, messagesImported: restored, messagesSkipped: skipped });
  });

  // ── RESYNC ─────────────────────────────────────────────────────────────────

  socket.on('resync', () => {
    const room = socket._room;
    if (!room || !rooms[room]) return;
    const everyone = rooms[room].members.map(u => ({
      socketId: u.socketId, username: u.username, pubKey: u.pubKey,
      joinedVia: u.joinedVia, homeServer: u.homeServer || null
    }));
    socket.emit('room-users-resync', everyone);
  });

  // ── LEAVE ROOM (explicit, from the Leave Room button) ─────────────────────
  // Without this, the server only learned a user left via socket disconnect,
  // which could be much later (or never, if they navigated away). Other room
  // members would see stale membership in the user list. Now the client emits
  // this whenever leaveRoom() runs and the server cleans up immediately.

  socket.on('leave-room', () => {
    const room     = socket._room;
    const username = socket._username;
    if (!room || !rooms[room]) return;
    rooms[room].members = rooms[room].members.filter(u => u.socketId !== socket.id);
    socket.to(room).emit('user-left', socket.id);
    clearSpotlightIfMember(rooms[room], room, username);
    if (lobbyUsers[username]) delete lobbyUsers[username].inRoom;
    socket.leave(room);
    socket._room = null;
    if (rooms[room].members.length === 0) {
      if (rooms[room]._capTimer)  clearTimeout(rooms[room]._capTimer);
      if (rooms[room]._warnTimer) clearTimeout(rooms[room]._warnTimer);
      delete rooms[room];
      chatDeleteRoom(room);
      console.log(`Room #${room} closed (last member left via leave-room)`);
    } else {
      console.log(`@${username} left #${room} via leave-room (${rooms[room].members.length} remaining)`);
    }
    broadcastRooms();
  });

  // ── WebRTC Signalling ──────────────────────────────────────────────────────

  socket.on('offer',         ({ to, offer })     => { io.to(to).emit('offer',         { from: socket.id, offer }); });
  socket.on('answer',        ({ to, answer })    => { io.to(to).emit('answer',        { from: socket.id, answer }); });
  socket.on('ice-candidate', ({ to, candidate }) => { io.to(to).emit('ice-candidate', { from: socket.id, candidate }); });

  // ── Room Chat ──────────────────────────────────────────────────────────────

  socket.on('chat-message', ({ room, to, from, ciphertext, broadcast, signature, timestamp }) => {
    if (broadcast) {
      socket.to(room).emit('chat-message', { from, to, ciphertext, broadcast: true, signature, timestamp });
    } else {
      if (!rooms[room]) return;
      const recipient = rooms[room].members.find(u => u.username === to);
      if (!recipient) return;
      io.to(recipient.socketId).emit('chat-message', { from, to, ciphertext, broadcast: false, signature, timestamp });
    }
    // Store in chat DB
    chatStoreRoomMsg(room, from, to, ciphertext, signature, timestamp, broadcast);
  });

  // ── Room attachments (ipfs-gate v0.1) ──────────────────────────────────────
  // Envelope shape: { v, type:'room-attachment', room, cid, size_bytes, sender,
  //   sender_pubkey, envelope_sig, created_at, expires_at, gateway_hint,
  //   kind_hint, per_recipient: { hive_account: encrypted_key_b64 } }
  // Server is router-only: file bytes never touch v4call. Sender is validated
  // as a current room member; recipients are looked up against the room's
  // current membership. Recipient clients verify envelope_sig themselves.
  socket.on('room-attachment', (env) => {
    try {
      if (!env || typeof env !== 'object') return;
      const room = env.room;
      const from = env.sender;
      if (!room || !from || !rooms[room]) return;
      if (socket._username !== from) return;                    // sender spoof guard
      if (!rooms[room].members.some(m => m.socketId === socket.id)) return;
      if (typeof env.cid !== 'string' || !env.cid.length) return;
      if (typeof env.envelope_sig !== 'string' || !env.envelope_sig.length) return;
      if (!env.per_recipient || typeof env.per_recipient !== 'object') return;

      // Broadcast envelope to all current room members (including sender for
      // their own bubble render). Recipients verify the sig client-side and
      // decrypt per_recipient[myUsername] if present; bystanders see a locked
      // bubble.
      io.to(room).emit('room-attachment', env);
      // Persist for room-history replay on rejoin. Envelope kept past
      // expires_at — the recipient's client gracefully surfaces ⚠ 404 when
      // the underlying pin is gone.
      chatStoreRoomAttachment(env);
      // v0.16.14 — Notify addressed recipients who aren't currently in the
      // room socket (e.g. in lobby or another room). They'll see a dot on
      // the room card / tab in their UI. Federated recipients (hosted on a
      // peer server) are skipped here — cross-server attachment notify is
      // a v0.3+ federation extension.
      try {
        const liveMembers = new Set((rooms[room].members || []).map(m => m.username));
        for (const recip of Object.keys(env.per_recipient || {})) {
          if (recip === from) continue;          // sender doesn't notify themselves
          if (liveMembers.has(recip)) continue;  // already got the live event
          const lu = lobbyUsers[recip];
          if (!lu || !lu.socketId) continue;     // offline → catches up on next rejoin via history
          io.to(lu.socketId).emit('attachment-notification', {
            room, sender: from, cid: env.cid
          });
        }
      } catch (e) {
        console.warn('[room-attachment] notify pass failed:', e.message);
      }
    } catch (e) {
      console.error('[room-attachment] handler error:', e.message);
    }
  });

  // ── DM attachments (ipfs-gate v0.1) ────────────────────────────────────────
  // Envelope shape: { v, type:'dm-attachment', to_user, cid, size_bytes,
  //   sender, sender_pubkey, envelope_sig, created_at, expires_at,
  //   gateway_hint, kind_hint, per_recipient: { sender: encKey, to_user: encKey },
  //   original_filename, original_mime, original_size,
  //   msgId?, textPaid?, textMemo?, textCurrency? }
  // Mirrors the lobby-dm paid gate: if the recipient has a text rate set, the
  // sender must include a verifiable on-chain payment before the envelope is
  // accepted. Disbursement (net to recipient, fee to platform) mirrors lobby-dm
  // exactly.
  // v0.16.18 — federation support. Caller's server is verifier + router;
  // recipient's server is treasurer. On a federated send the caller verifies
  // the on-chain payment to the recipient's declared escrow (which lives on
  // the peer's server), then forwards via federation `dm-attachment`. The
  // recipient's server re-validates per design rule #15 and disburses.
  socket.on('dm-attachment', async (env) => {
    try {
      if (!env || typeof env !== 'object') return;
      const from = env.sender;
      const to   = env.to_user;
      if (!from || !to) return;
      if (socket._username !== from) return;                       // sender spoof guard
      if (typeof env.cid !== 'string' || !env.cid.length) return;
      if (typeof env.envelope_sig !== 'string' || !env.envelope_sig.length) return;
      if (!env.per_recipient || typeof env.per_recipient !== 'object') return;
      if (!(from in env.per_recipient) || !(to in env.per_recipient)) {
        socket.emit('dm-attachment-error', { msgId: env.msgId || null, error: 'envelope must address both sender and recipient' });
        return;
      }

      const recipient   = lobbyUsers[to];
      const federatedTo = recipient ? null : (FEDERATION_ENABLED ? peerForUser(to) : null);
      if (!recipient && !federatedTo) {
        // Same Nostr-only distinction as lobby-dm. Critical for attachments
        // because by the time the server sees the envelope, the sender has
        // already paid BOTH the paid-DM rate AND the ipfs-gate CNOOBS/TEST
        // fee. Surface the transient state clearly so the user knows to retry
        // rather than thinking the payment was wasted.
        const nostrDomain = nostrSeenDomain(to);
        if (nostrDomain) {
          const errText = FEDERATION_ENABLED
            ? `⏳ @${to} is shown online via Nostr presence (${nostrDomain}), but federation is still reconnecting. Attachment is uploaded — try sending again in ~30s and your payment will route through.`
            : `@${to} is on ${nostrDomain} (visible via Nostr presence), but this server has WS server-to-server federation disabled — cross-server attachments aren't routable. Ask the operator to enable FEDERATION_PEERS in .env.`;
          socket.emit('dm-attachment-error', { msgId: env.msgId || null, error: errText });
          return;
        }
        socket.emit('dm-attachment-error', { msgId: env.msgId || null, error: `@${to} is not online` });
        return;
      }

      // Resolve recipient's text rate against this sender (same path lobby-dm uses)
      const calleeRates = await fetchRates(to);
      const applicable  = calleeRates
        ? await getRatesForCaller(calleeRates, from, 'text', new Date())
        : null;

      if (applicable?.blocked) {
        socket.emit('dm-attachment-error', { msgId: env.msgId || null, error: `🚫 ${applicable.message || 'You have been blocked by this user.'}` });
        return;
      }
      if (applicable?.feeRejected) {
        socket.emit('dm-attachment-error', { msgId: env.msgId || null, error: `⚠ ${applicable.message}` });
        return;
      }

      const textRate     = applicable?.flat || 0;
      const cur          = env.textCurrency || applicable?.currency || 'HBD';
      const calleeEscrow = calleeRates?.escrow || ESCROW_ACCOUNT;

      // Escrow-mismatch guard for paid federated attachments — mirrors lobby-dm.
      // If the recipient's rate-post escrow isn't controlled by the peer, the
      // peer can't disburse and the caller's funds would orphan. Fail loudly.
      if (federatedTo && textRate >= RATE_FLOOR && federatedTo.escrow && federatedTo.escrow !== calleeEscrow) {
        socket.emit('dm-attachment-error', {
          msgId: env.msgId || null,
          error: `⚠ @${to}'s rates post declares escrow @${calleeEscrow}, but ${federatedTo.domain} controls @${federatedTo.escrow}. They need to update their rates post for paid attachments to work across federation.`
        });
        return;
      }

      if (textRate >= RATE_FLOOR) {
        // Payment required
        if (!env.textPaid || !env.textMemo || !env.msgId) {
          socket.emit('dm-attachment-payment-required', {
            msgId:   env.msgId || null,
            to,
            rate:    textRate,
            currency: cur,
            escrow:  calleeEscrow
          });
          return;
        }

        // Caller-side on-chain verification against the recipient's declared
        // escrow (the peer's escrow for federated, or our own for local).
        const ok = (cur !== 'HBD' && cur !== 'HIVE')
          ? await verifyHiveEnginePayment(from, calleeEscrow, env.textPaid, cur, env.textMemo)
          : await verifyHivePayment(from, calleeEscrow, env.textPaid, env.textMemo);
        if (!ok) {
          socket.emit('dm-attachment-error', { msgId: env.msgId, error: `Payment not found on blockchain — attachment not sent. Funds are safe if ${cur} left your account.` });
          return;
        }

        ledgerPayment(env.msgId, 'text', from, calleeEscrow, env.textPaid, env.textMemo, 'verified', null, cur);

        // Disbursement ONLY runs on the local-recipient path. For federated
        // recipients the peer's server owns the escrow and does the disburse
        // after its own re-verification (per design rule #15).
        if (recipient) {
          const platformFee  = applicable.platformFee || PLATFORM_FEE;
          const platformCut  = parseFloat((env.textPaid * platformFee).toFixed(3));
          const recipientNet = parseFloat((env.textPaid - platformCut).toFixed(3));

          if (recipientNet >= 0.001) {
            const payoutMemo = `v4call:dm-att-payout:${env.msgId}`;
            ledgerPayment(env.msgId, 'text_payout', ESCROW_ACCOUNT, to, recipientNet, payoutMemo, 'pending', null, cur);
            sendFromEscrow(to, recipientNet, payoutMemo, cur, env.msgId).then(r => {
              if (r.success) {
                ledgerPaymentUpdate(env.msgId, 'text_payout', 'sent', r.txId);
                io.to(recipient.socketId).emit('text-payment-received', {
                  from, amount: recipientNet, currency: cur, msgId: env.msgId
                });
              } else {
                console.error(`[dm-attachment] Payout failed to @${to}: ${r.reason}`);
              }
            });
          }

          if (platformCut >= 0.001) {
            const feeMemo = `v4call:dm-att-fee:${env.msgId}`;
            ledgerPayment(env.msgId, 'text_fee', ESCROW_ACCOUNT, SERVER_HIVE_ACCOUNT, platformCut, feeMemo, 'pending', null, cur);
            sendFromEscrow(SERVER_HIVE_ACCOUNT, platformCut, feeMemo, cur, env.msgId).then(r => {
              if (r.success) ledgerPaymentUpdate(env.msgId, 'text_fee', 'sent', r.txId);
            });
          }

          console.log(`[dm-attachment] @${from} → @${to}: paid ${env.textPaid} ${cur} | net: ${recipientNet} | fee: ${platformCut}`);
        } else {
          console.log(`[dm-attachment] @${from} → @${to}@${federatedTo.domain}: paid ${env.textPaid} ${cur} (forwarding to peer for disburse)`);
        }
      }

      // Build the wire envelope (no payment fields — those are server-side
      // routing data, not part of the attachment envelope itself).
      const { msgId: _m, textPaid: _p, textMemo: _t, textCurrency: _c, ...wireEnv } = env;

      if (recipient) {
        // Local delivery — emit to recipient, echo to sender.
        io.to(recipient.socketId).emit('dm-attachment', wireEnv);
        socket.emit('dm-attachment', wireEnv);
      } else {
        // Federated delivery — forward via federation. Echo to sender locally
        // so their bubble renders immediately; the peer will deliver to the
        // recipient. On peer-side reject we get back `dm-attachment-failed`
        // which surfaces as an error to the sender via the federation handler.
        fedSend(federatedTo.ws, {
          type: 'dm-attachment',
          from, to,
          envelope: wireEnv,
          msgId:        env.msgId        || null,
          textPaid:     env.textPaid     || 0,
          textMemo:     env.textMemo     || null,
          textCurrency: cur,
          fromServer:   SERVER_DOMAIN
        });
        socket.emit('dm-attachment', wireEnv);
      }

      // Always persist sender's local copy so sender's history-replay on login
      // includes it (their wrapped key is in per_recipient — they can decrypt
      // their own copy). For federated, the peer separately persists the
      // recipient's copy on its side.
      chatStoreDmAttachment(env, env.textPaid || 0, cur);
      socket.emit('dm-attachment-sent', { msgId: env.msgId || null, to, textPaid: env.textPaid || 0, textCurrency: cur });
    } catch (e) {
      console.error('[dm-attachment] handler error:', e.message);
      try { socket.emit('dm-attachment-error', { msgId: env?.msgId || null, error: e.message }); } catch (_) {}
    }
  });

  socket.on('dm-attachments-history', ({ withUser }, cb) => {
    const username = socket._username;
    if (!username || !withUser) return cb ? cb([]) : null;
    const history = chatGetDmAttachments(username, withUser);
    if (cb) cb(history);
    else socket.emit('dm-attachments-history', { withUser, envelopes: history });
  });

  // v0.16.18 — Pre-flight routing check used by the client before any paid
  // action (paid-DM Keychain prompt, ipfs-gate CNOOBS transfer). Returns the
  // recipient's current routing status so the client can abort without
  // charging when the user is visible via Nostr but WS federation isn't
  // connected yet. Without this, a paid-DM or paid-attachment to a Nostr-only
  // user would burn both fees with no delivery.
  socket.on('dm-precheck', ({ to }, cb) => {
    if (!socket._username || !to) {
      if (cb) cb({ status: 'offline' });
      return;
    }
    const s = recipientStatus(String(to).toLowerCase());
    // Strip non-serializable internals before responding.
    // federationEnabled tells the client whether WS server-to-server fed is
    // even configured — when false and the recipient is nostr-only, there's
    // literally no transport (don't tell the user to "wait for reconnect").
    if (s.status === 'local')     { if (cb) cb({ status: 'local' });                     return; }
    if (s.status === 'federated') { if (cb) cb({ status: 'federated', domain: s.peer.domain }); return; }
    if (s.status === 'nostr-only'){ if (cb) cb({ status: 'nostr-only', domain: s.domain, federationEnabled: FEDERATION_ENABLED }); return; }
    if (cb) cb({ status: 'offline' });
  });

  // ── DISCONNECT ─────────────────────────────────────────────────────────────

  socket.on('disconnect', () => {
    const username = socket._username;
    const room     = socket._room;

    // Clean up any pending (ringing) call
    if (socket._pendingCall) {
      const { roomName, callee } = socket._pendingCall;
      if (rooms[roomName]?._callTimer) clearTimeout(rooms[roomName]._callTimer);
      if (rooms[roomName]?.members.length === 0) delete rooms[roomName];
      if (lobbyUsers[callee]) delete lobbyUsers[callee].inCall;
      socket._pendingCall = null;
    }

    if (room && rooms[room]) {
      rooms[room].members = rooms[room].members.filter(u => u.socketId !== socket.id);
      socket.to(room).emit('user-left', socket.id);
      clearSpotlightIfMember(rooms[room], room, username);

      if (rooms[room].isCall) {
        console.log(`[disconnect] @${username} disconnected from 1:1 call room #${room} → emitting peer-hung-up`);
        socket.to(room).emit('peer-hung-up', { by: username });
        const callId = rooms[room].callId;
        if (callId && activePayments[callId]) {
          processCallEnd(callId, room, io, lobbyUsers);
        }
      } else {
        console.log(`[disconnect] @${username} disconnected from multi-party room #${room} (${rooms[room].members.length} remaining) — no peer-hung-up emitted`);
      }

      if (rooms[room].members.length === 0) {
        if (rooms[room]._capTimer)  clearTimeout(rooms[room]._capTimer);
        if (rooms[room]._warnTimer) clearTimeout(rooms[room]._warnTimer);
        delete rooms[room];
        chatDeleteRoom(room); // Clean up stored messages — room is ephemeral
        console.log(`Room #${room} closed`);
      }
      broadcastRooms();
    }

    // v0.16.17 — Reconnect grace: defer the user-offline broadcast by 5s in
    // case this is a transient drop (laptop sleep, wifi blip, hot Socket.io
    // reconnect). Without this, every drop generated a flap in the lobby user
    // list + active-rooms count visible to everyone else. If the user
    // reconnects within the window, lobby-join cancels the timer and nobody
    // sees the flap. Past 5s, the original offline broadcast fires as usual.
    // Only the most-recent socket for a username (the canonical lobby presence)
    // triggers the timer — earlier-tab disconnects in a multi-tab session are
    // ignored for offline-broadcast purposes, mirroring pre-v0.16.17 behavior.
    if (username && lobbyUsers[username] && lobbyUsers[username].socketId === socket.id) {
      if (pendingOfflineTimers[username]) clearTimeout(pendingOfflineTimers[username]);
      pendingOfflineTimers[username] = setTimeout(() => {
        delete pendingOfflineTimers[username];
        // Re-check that no reconnect snuck in between scheduling + firing.
        // If lobbyUsers[username] was replaced by a fresh tab, that tab's
        // own lobby-join already handled the broadcast — skip.
        if (lobbyUsers[username] && lobbyUsers[username].socketId !== socket.id) return;
        delete lobbyUsers[username];
        broadcastLobby();
        broadcastRooms();
        if (FEDERATION_ENABLED) fedAnnounceUserOffline(username);
      }, 5000);
    }

    console.log(`Disconnected: @${username || '?'} (${socket.id})`);
  });
});


// ─────────────────────────────────────────────────────────────────────────────
// ── Federation (server-to-server) ─────────────────────────────────────────────
//
// Two v4call servers discover each other via hardcoded FEDERATION_PEERS URLs
// and exchange a small set of JSON messages over a persistent WebSocket:
//
//   hello        — identify self (domain + version)
//   presence     — full user list snapshot (sent on connect)
//   user-online  — incremental add to peer's user list
//   user-offline — incremental remove
//   dm           — relay an encrypted DM (ciphertext only — server never sees plaintext)
//   call-invite  — ring a federated user (caller's server hosts the room)
//   call-response / call-declined / call-cancelled / call-missed — call lifecycle
//
// Both servers connect outbound to each other AND accept inbound connections.
// The most recent connection per domain wins (old one gets closed).
// ─────────────────────────────────────────────────────────────────────────────

function fedSend(ws, msg) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(msg)); } catch(e) { /* ignore — peer will reconnect */ }
  }
}

// v0.16 Part B — when a federation peer drops, kick every federated member
// whose homeServer matches that domain from every room hosted on this server.
// Per the (a) immediate-eviction design call: rooms have no payment to refund
// and the temp Socket.io can survive a federation drop, but we choose clean
// state over zombie members. The kicked event reaches the federated user via
// their (still-alive) temp Socket.io, their client closes the temp socket,
// and they end up back in their home server's lobby.
function cleanupFederatedMembersForPeer(domain) {
  let totalRemoved = 0;
  for (const [roomName, r] of Object.entries(rooms)) {
    const removed = r.members.filter(m => m.homeServer === domain);
    if (removed.length === 0) continue;
    const survivors = r.members.filter(m => m.homeServer !== domain);
    for (const m of removed) {
      io.to(m.socketId).emit('kicked', { room: roomName, reason: `Federation connection to ${domain} was lost — you've been removed from this room.` });
      io.to(roomName).emit('user-left', m.socketId);
      clearSpotlightIfMember(r, roomName, m.username);
      totalRemoved++;
    }
    r.members = survivors;
    if (r.members.length === 0) {
      if (r._capTimer)  clearTimeout(r._capTimer);
      if (r._warnTimer) clearTimeout(r._warnTimer);
      delete rooms[roomName];
      chatDeleteRoom(roomName);
      console.log(`[federation] Room #${roomName} closed after federation drop to ${domain}`);
    } else {
      emitRoomInfoToMembers(r, roomName);
      console.log(`[federation] Removed ${removed.length} federated member(s) from #${roomName} after federation drop to ${domain}`);
    }
  }
  if (totalRemoved > 0) broadcastRooms();
}

// True if the peer is currently connected AND advertised protocol_version >= 0.4.
// v0.3 peers don't send protocol_version, so they fail this check — that's the
// gate for v0.4-only features (cross-server room invites + responses).
function peerSupportsV04(domain) {
  const peer = federationPeers[domain];
  if (!peer || !peer.connected) return false;
  const n = parseFloat(peer.protocolVersion);
  return !isNaN(n) && n >= 0.4;
}

// Lower-case, strip leading @, split on '@'. For local form returns server=null.
// For 'cnoobz@hive-book.com' returns { user:'cnoobz', server:'hive-book.com' }.
// Returns null on malformed input (empty user, empty server, multiple @s).
function parseFederatedHandle(input) {
  const norm = (input || '').trim().toLowerCase().replace(/^@/, '');
  if (!norm) return null;
  const parts = norm.split('@');
  if (parts.length === 1) return { user: parts[0], server: null };
  if (parts.length !== 2) return null;
  if (!parts[0] || !parts[1]) return null;
  return { user: parts[0], server: parts[1] };
}

function fedBroadcast(msg) {
  for (const peer of Object.values(federationPeers)) {
    if (peer.connected) fedSend(peer.ws, msg);
  }
}

// Tiebreaker: when both peers initiate outbound connections at the same time,
// only the lexicographically-smaller domain keeps its outbound. The other peer
// closes its own outbound and accepts the inbound from the smaller-domain side.
// Both ends use the same comparison, so they agree on which TCP connection
// survives — no flapping.
function fedShouldInitiate(peerDomain) {
  return SERVER_DOMAIN.toLowerCase().localeCompare(peerDomain.toLowerCase()) < 0;
}

// ── Peer verification via signed /.well-known/v4call-server.json ─────────────
// During the hello handshake, fetch the peer's signed ownership file from its
// claimed domain and verify it was signed by the Hive account it claims to
// belong to. This ties Hive identity to domain control: a squatter can spoof
// hello but can't serve a matching file with a valid signature on a domain
// they don't control. Results cache to avoid hammering peers on every connect.
const PEER_VERIFY_TTL_OK         = 60 * 60 * 1000;   // 1h for positive results
const PEER_VERIFY_TTL_STRUCTURAL = 5  * 60 * 1000;   // 5min for permanent failures (bad sig, wrong claim, domain mismatch)
const PEER_VERIFY_TTL_TRANSIENT  = 30 * 1000;        // 30s for transient failures (timeout, network error, HTTP 5xx)
const peerVerifyCache = {};                          // domain → { result, cachedAt, transient? }

// Bumped 8s → 15s in v0.16.19 after a production cascade on v4call.com:
// out-of-band curl/wget against the same peers returned in ~70ms, but the
// in-process Node fetch hit the 8s AbortSignal repeatedly. Working hypothesis
// is event-loop pressure (heavy concurrent fed handshakes / Nostr publishes /
// Hive scans block the loop long enough for the timeout to fire before the
// fetch's I/O actually runs). 15s is a forgiving budget that still catches
// genuine network failure quickly. The cache-TTL split below is the real fix —
// this is the smaller half of the pair.
async function _fetchPeerVerifyFile(domain) {
  const url = `https://${domain}/.well-known/v4call-server.json`;
  const res = await fetch(url, { signal: AbortSignal.timeout(15000) });
  if (!res.ok) {
    const err = new Error(`HTTP ${res.status}`);
    // Tag the error so verifyPeer can classify the failure category. 5xx is
    // transient (peer overloaded), 4xx is structural (peer mis-served the
    // well-known on purpose or by config).
    err._transient = res.status >= 500;
    throw err;
  }
  return await res.json();
}

async function _lookupPostingPubKey(account) {
  const data = await hivePost({
    jsonrpc: '2.0', method: 'condenser_api.get_accounts',
    params: [[account]], id: 1
  });
  if (!data?.result?.length) return null;
  return data.result[0].posting?.key_auths?.[0]?.[0] || null;
}

// Canonical signed payload — MUST match public/server-sign.html `buildPayload`
// byte-for-byte, because we recompute it here to verify the Hive signature.
//
// "Option B" (nGate STATUS.md, 2026-05-13):
//   • 9-field shape  → when the well-known file has NO Nostr fields populated.
//   • 12-field shape → when ANY of nostr_npub / nostr_hex / nostr_relays is set,
//     a 3-field Nostr trailer is appended: nostr_npub | nostr_hex | relays_csv.
//   • The nostr_attestation block is NEVER included in the Hive canonical
//     (the attestation is self-verifying via its own NIP-01 id + schnorr sig).
//
// If you change the order/shape here, you MUST change the matching block in
// server-sign.html, OR every v4call server stops verifying every other
// v4call server's well-known. Cross-checked by comparing the joined string
// for the same input — keep them byte-identical.
//
// Backward compat: a file with no Nostr fields produces the 9-field shape
// exactly as before, so peers signed pre-Nostr keep verifying without re-sign.
function _verifyPayloadString(obj) {
  const base = [
    obj.claim, obj.domain, obj.hive_account,
    obj.escrow, obj.fee_account, obj.federation_ws,
    obj.issued, obj.expires || '', obj.nonce
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

async function verifyPeer(domain, claimedAccount) {
  // Cache lookup honours the failure category written at insert time. Positive
  // results live 1h; structural failures (bad sig / wrong claim / etc.) live
  // 5min because they can't change without operator action; transient failures
  // (network timeout / 5xx / Hive node hiccup) live only 30s so a single bad
  // fetch can't cascade into a 5-minute outage.
  const cached = peerVerifyCache[domain];
  if (cached) {
    let ttl;
    if (cached.result.verified)  ttl = PEER_VERIFY_TTL_OK;
    else if (cached.transient)   ttl = PEER_VERIFY_TTL_TRANSIENT;
    else                         ttl = PEER_VERIFY_TTL_STRUCTURAL;
    if ((Date.now() - cached.cachedAt) < ttl) return cached.result;
  }

  // `transient = true` → cached for 30s (retry quickly on next handshake).
  // `transient = false` → cached for 5min (won't change until operator fixes).
  const fail = (reason, transient = false) => {
    const r = { verified: false, reason };
    peerVerifyCache[domain] = { result: r, cachedAt: Date.now(), transient };
    return r;
  };

  let obj;
  try { obj = await _fetchPeerVerifyFile(domain); }
  catch(e) {
    // AbortSignal timeouts, DNS errors, ECONNREFUSED, ECONNRESET, HTTP 5xx are
    // all transient. HTTP 4xx (peer's nginx mis-served the file) is structural.
    const isTransient = e._transient !== false; // _transient set in fetcher; default to true for network errors
    return fail(`Cannot fetch v4call-server.json: ${e.message}`, isTransient);
  }

  // Shape + field-consistency checks — all structural (won't change until peer
  // operator regenerates the well-known file).
  if (obj.claim !== 'v4call-server-ownership')                 return fail('wrong claim', false);
  if (!obj.domain || !obj.hive_account || !obj.signature)      return fail('missing required fields', false);
  if (obj.domain.toLowerCase() !== domain.toLowerCase())       return fail(`domain mismatch (file claims ${obj.domain})`, false);
  if (claimedAccount && obj.hive_account.toLowerCase() !== claimedAccount.toLowerCase()) {
    return fail(`account mismatch: hello says @${claimedAccount} but file signed by @${obj.hive_account}`, false);
  }

  // Expiry — only enforce if operator set one. Structural — peer needs to
  // re-sign to fix.
  if (obj.expires) {
    const exp = Date.parse(obj.expires);
    if (Number.isFinite(exp) && Date.now() > exp) return fail(`expired on ${obj.expires}`, false);
  }
  // Reject far-future issued (tolerates 5min clock skew). Structural.
  if (obj.issued) {
    const iss = Date.parse(obj.issued);
    if (Number.isFinite(iss) && iss > Date.now() + 5 * 60 * 1000) return fail(`issued timestamp in future: ${obj.issued}`, false);
  }

  // Signature verification — Hive lookup is transient (RPC hiccup), but
  // missing-pubkey or actual mismatch is structural.
  let pubKeyStr;
  try { pubKeyStr = await _lookupPostingPubKey(obj.hive_account); }
  catch(e) { return fail(`Hive lookup error: ${e.message}`, true); }
  if (!pubKeyStr) return fail(`no posting key found for @${obj.hive_account}`, false);

  try {
    const payload = _verifyPayloadString(obj);
    const hash    = dhive.cryptoUtils.sha256(payload);
    const pubKey  = dhive.PublicKey.fromString(pubKeyStr);
    const sig     = dhive.Signature.fromString(obj.signature);
    if (!pubKey.verify(hash, sig)) return fail('signature does not match posting key', false);
  } catch(e) {
    return fail(`signature parse error: ${e.message}`, false);
  }

  // Hive-anchored Nostr binding (only meaningful when the file used the
  // 12-field canonical and the signature just verified). nostr_hex here is
  // now covered by the Hive posting-key signature, so a future Phase D
  // presence handler can trust `event.pubkey === peer.verified_nostr_hex`
  // as a Hive-rooted check (no Phase C "poke" indirection needed).
  const ok = {
    verified:      true,
    domain:        obj.domain,
    hive_account:  obj.hive_account,
    escrow:        obj.escrow,
    fee_account:   obj.fee_account,
    federation_ws: obj.federation_ws,
    issued:        obj.issued,
    expires:       obj.expires || null,
    verified_nostr_hex: obj.nostr_hex || null,
    verified_nostr_npub: obj.nostr_npub || null,
  };
  peerVerifyCache[domain] = { result: ok, cachedAt: Date.now() };
  return ok;
}

// ── Directory scanner: Hive-tag discovery of other v4call servers ────────────
// Parses [V4CALL-SERVER-V1] blocks out of posts tagged "v4call-server" on Hive,
// keeps the most recent post per author, verifies each candidate via the same
// verifyPeer() used at handshake, and populates discoveredPeers for the admin
// UI. Does NOT auto-approve — operator reviews and calls /admin/peers/approve.
// ─────────────────────────────────────────────────────────────────────────────

function parseV4CallServerPost(body) {
  if (!body) return null;
  const m = body.match(/\[V4CALL-SERVER-V1\]([\s\S]*?)\[\/V4CALL-SERVER-V1\]/i);
  if (!m) return null;
  const block = m[1];
  const grab = (key) => {
    const re = new RegExp(`^\\s*${key}:\\s*(.+?)\\s*$`, 'mi');
    const mm = block.match(re);
    return mm ? mm[1].trim() : '';
  };
  const out = {
    domain:        grab('DOMAIN').toLowerCase(),
    hive_account:  grab('HIVE-ACCOUNT').toLowerCase().replace(/^@/, ''),
    escrow:        grab('ESCROW').toLowerCase().replace(/^@/, ''),
    fee_account:   grab('FEE-ACCOUNT').toLowerCase().replace(/^@/, ''),
    federation_ws: grab('FEDERATION-WS'),
    verify_url:    grab('VERIFY-URL'),
    software:      grab('SOFTWARE'),
    protocol:      grab('PROTOCOL'),
    declared:      grab('DECLARED')
  };
  if (!out.domain || !out.hive_account) return null;
  return out;
}

async function scanV4CallDirectory() {
  console.log('[discovery] Scanning Hive tag "v4call-server" for federation peers…');
  // get_discussions_by_created returns posts under the tag sorted by recency.
  // (get_discussions_by_tag doesn't exist on Hive's condenser API — that was
  // a wrong method name that returned an assert exception silently.)
  // Hive nodes enforce a max `limit` of 20 (Assert Exception otherwise) — so
  // the most recent 20 v4call-server posts get scanned each cycle. Once there
  // are more than ~20 active v4call servers, swap in start_author/permlink
  // pagination here.
  const data = await hivePost({
    jsonrpc: '2.0',
    method:  'condenser_api.get_discussions_by_created',
    params:  [{ tag: 'v4call-server', limit: 20 }],
    id: 1
  });
  if (!data?.result) {
    console.warn('[discovery] No response from Hive tag query');
    return;
  }
  console.log(`[discovery] Hive returned ${data.result.length} post(s) under v4call-server tag`);

  // Keep only the most recent post PER AUTHOR with title "v4call-server".
  // Stray tag usage (random posts that happen to include the tag) is filtered
  // out here — we insist the canonical title as a sanity check.
  const byAuthor = {};
  for (const post of data.result) {
    if (!post.title || post.title.toLowerCase() !== 'v4call-server') continue;
    const existing = byAuthor[post.author];
    if (!existing || new Date(post.created) > new Date(existing.created)) {
      byAuthor[post.author] = post;
    }
  }

  let parsedCount = 0, verifiedCount = 0;
  const now = new Date().toISOString();
  for (const post of Object.values(byAuthor)) {
    const parsed = parseV4CallServerPost(post.body);
    if (!parsed) continue;
    parsedCount++;
    if (parsed.hive_account !== post.author.toLowerCase()) {
      console.warn(`[discovery] Post author mismatch: @${post.author} claims HIVE-ACCOUNT @${parsed.hive_account} — ignored`);
      continue;
    }
    // Re-verify each discovered peer. verifyPeer is cached (1h positive / 5m
    // negative) so rescans are cheap for unchanged peers.
    const vr = await verifyPeer(parsed.domain, parsed.hive_account);
    discoveredPeers[parsed.domain] = {
      post_author:   post.author,
      post_permlink: post.permlink,
      post_created:  post.created,
      parsed,
      verified:      !!vr.verified,
      verify_reason: vr.verified ? null : (vr.reason || 'unknown'),
      // Hive-anchored Nostr identity (only set when the verify file carries
      // the 12-field canonical with Nostr trailer). Phase D presence rides
      // on this without needing the Phase C "poke" indirection.
      verified_nostr_hex:  vr.verified ? (vr.verified_nostr_hex || null)  : null,
      verified_nostr_npub: vr.verified ? (vr.verified_nostr_npub || null) : null,
      last_seen:     now
    };
    if (vr.verified) verifiedCount++;
  }
  console.log(`[discovery] Scan complete — ${parsedCount} v4call-server post(s), ${verifiedCount} verified`);
}

// ── Nostr-triggered discovery (Phase C) ─────────────────────────────────────
// Called by nostr-fed.mjs when a v4call-server kind-30078 event arrives. We
// trust NOTHING in the Nostr event payload — we extract only the domain and
// run the existing verifyPeer() which is Hive-signature-anchored. The Nostr
// event is effectively a "poke" that says "re-check this domain now" instead
// of waiting up to 2h for the Hive scan. Worst case a forged event can do is
// trigger a Hive verify we'd have done in the next scan anyway.
//
// Precedence: if a Hive-scan entry already exists for this domain (has
// post_author), we KEEP it and only refresh last_seen — Hive-scan entries
// are richer (carry post_*) and we don't want Nostr to clobber that.
async function discoverPeerViaNostr({ domain, pubkey, eventId, content }) {
  try {
    if (!domain) return;
    domain = String(domain).toLowerCase();
    const existing = discoveredPeers[domain];
    const now = new Date().toISOString();

    // If a Hive-scan-sourced entry already exists, don't degrade it — just
    // record that we saw it via Nostr too.
    if (existing && existing.post_author) {
      existing.last_seen   = now;
      existing.nostr_seen  = true;
      existing.nostr_pubkey = pubkey || existing.nostr_pubkey || null;
      console.log(`[discovery] nostr poke for @${domain} — already known via Hive scan, last_seen refreshed`);
      return;
    }

    // Verify the domain via the EXISTING Hive-anchored verifyPeer (no claimed
    // account — let the signed file declare its own hive_account and we'll
    // accept whatever the Hive signature validates).
    const vr = await verifyPeer(domain);

    // Build a `parsed` shape compatible with the admin-peers UI + approve path.
    // Security-relevant fields come from the verified well-known (vr); the
    // display-only fields can come from the Nostr event content (untrusted,
    // labelled clearly in the source field).
    const parsed = vr.verified ? {
      domain:        vr.domain,
      hive_account:  vr.hive_account,
      escrow:        vr.escrow,
      fee_account:   vr.fee_account,
      federation_ws: vr.federation_ws,
      verify_url:    `https://${domain}/.well-known/v4call-server.json`,
      software:      (content && content.software) || 'unknown',
      protocol:      (content && content.protocol) || 'unknown',
      declared:      (content && content.announced_at) || null,
    } : {
      domain,
      hive_account:  (content && content.hive_account) || null,
      escrow:        null, fee_account: null, federation_ws: null,
      verify_url:    `https://${domain}/.well-known/v4call-server.json`,
      software:      (content && content.software) || 'unknown',
      protocol:      (content && content.protocol) || 'unknown',
      declared:      (content && content.announced_at) || null,
    };

    discoveredPeers[domain] = {
      // Hive-post fields blank — this entry came from Nostr.
      post_author:   '',
      post_permlink: '',
      post_created:  null,
      // Phase C source-tracking fields (admin UI ignores unknown keys safely).
      source:        'nostr',
      nostr_pubkey:  pubkey || null,
      nostr_event_id: eventId || null,
      parsed,
      verified:      !!vr.verified,
      verify_reason: vr.verified ? null : (vr.reason || 'unknown'),
      // Hive-anchored Nostr identity (only set when the verified file carries
      // the 12-field canonical). Phase D rides on this directly.
      verified_nostr_hex:  vr.verified ? (vr.verified_nostr_hex || null)  : null,
      verified_nostr_npub: vr.verified ? (vr.verified_nostr_npub || null) : null,
      last_seen:     now
    };

    if (vr.verified) {
      console.log(`[discovery] ✓ nostr-discovered + verified: @${domain} (signer @${vr.hive_account}) — visible in /admin-peers.html for approval`);
    } else {
      console.log(`[discovery] ✗ nostr-discovered but unverified: @${domain} — ${vr.reason}`);
    }
  } catch (e) {
    // Discovery failures must never propagate. The Hive scan will retry.
    console.error('[discovery] nostr discover error (non-fatal):', e.message);
  }
}

function fedRegisterPeer(domain, name, ws, escrow) {
  const existing = federationPeers[domain];
  const isOutbound = ws._isOutbound === true;

  if (existing && existing.ws && existing.ws !== ws && existing.ws.readyState === WebSocket.OPEN) {
    // We already have a healthy connection for this peer. Decide which one
    // survives based on domain tiebreaker so both sides agree.
    const wePreferOutbound = fedShouldInitiate(domain);
    const newMatchesPref      = (isOutbound === wePreferOutbound);
    const existingMatchesPref = (existing.isOutbound === wePreferOutbound);

    if (newMatchesPref && !existingMatchesPref) {
      // Replace the existing socket — new one is the preferred direction.
      try { existing.ws.close(1000, 'superseded'); } catch(_) {}
    } else {
      // Reject duplicate — keep the existing.
      try { ws.close(1000, 'duplicate'); } catch(_) {}
      return false;
    }
  }

  federationPeers[domain] = {
    ws,
    connected: true,
    name:     name   || domain,
    escrow:   escrow || (existing ? existing.escrow : null),
    users:    existing ? existing.users : new Map(),
    isOutbound
  };
  return true;
}

function fedSendLocalPresence(ws) {
  const users = Object.entries(lobbyUsers)
    .filter(([, u]) => !u.invisible)
    .map(([username, u]) => ({ username, pubKey: u.pubKey }));
  fedSend(ws, { type: 'presence', users });
}

function fedAnnounceUserOnline(username) {
  const u = lobbyUsers[username];
  if (!u || u.invisible) return;
  const peerCount = Object.values(federationPeers).filter(p => p.connected).length;
  console.log(`[federation] → user-online @${username} → ${peerCount} peer(s)`);
  fedBroadcast({ type: 'user-online', username, pubKey: u.pubKey });
}

function fedAnnounceUserOffline(username) {
  const peerCount = Object.values(federationPeers).filter(p => p.connected).length;
  console.log(`[federation] → user-offline @${username} → ${peerCount} peer(s)`);
  fedBroadcast({ type: 'user-offline', username });
}

async function fedHandleMessage(ws, raw) {
  let msg;
  try { msg = JSON.parse(raw); } catch(e) { return; }
  if (!msg || !msg.type) return;

  switch (msg.type) {
    case 'hello': {
      const domain         = (msg.domain || '').toLowerCase();
      const claimedAccount = (msg.hive_account || '').toLowerCase();
      if (!domain) return;
      ws._domain = domain;

      // Cryptographic proof-of-ownership. Fails closed — unverified peers
      // cannot federate. See verifyPeer() for the rules.
      const vr = await verifyPeer(domain, claimedAccount);
      if (!vr.verified) {
        console.error(`[federation] ✗ Peer verification failed for ${domain}: ${vr.reason}`);
        try { ws.close(1008, 'verification failed'); } catch(_) {}
        return;
      }

      // Approval gate — verified peers must also be explicitly trusted by
      // this operator before they can federate. Seeded from FEDERATION_PEERS
      // env; additions via POST /admin/peers/approve after reviewing the
      // discovered-peers list.
      if (!approvedPeers.has(domain)) {
        console.warn(`[federation] ✗ Peer verified but NOT APPROVED: ${domain} — approve via POST /admin/peers/approve?domain=${domain}&key=<ADMIN_KEY>`);
        try { ws.close(1008, 'not approved'); } catch(_) {}
        return;
      }

      // Use the SIGNED values as authoritative — hello fields are unsigned.
      const registered = fedRegisterPeer(domain, msg.name, ws, vr.escrow);
      if (!registered) return; // duplicate rejected by tiebreaker
      const peer = federationPeers[domain];
      peer.verified        = true;
      peer.hive_account    = vr.hive_account;
      peer.fee_account     = vr.fee_account;
      peer.issued          = vr.issued;
      peer.expires         = vr.expires;
      peer.protocolVersion = (typeof msg.protocol_version === 'string') ? msg.protocol_version : null;

      if (msg.escrow && msg.escrow !== vr.escrow) {
        console.warn(`[federation] ⚠ ${domain}: hello.escrow=@${msg.escrow} but v4call-server.json.escrow=@${vr.escrow} — using signed value`);
      }

      console.log(`[federation] ✓ Peer verified: @${domain} (signer: @${vr.hive_account}, escrow: @${vr.escrow}${vr.expires ? `, expires ${vr.expires}` : ''})`);
      fedSendLocalPresence(ws);
      broadcastLobby();
      break;
    }
    case 'presence': {
      if (!ws._domain) return;
      const peer = federationPeers[ws._domain];
      if (!peer) return;
      peer.users = new Map();
      for (const u of (msg.users || [])) {
        if (u.username) peer.users.set(u.username, { pubKey: u.pubKey || '' });
      }
      console.log(`[federation] Presence from ${ws._domain}: ${peer.users.size} users`);
      broadcastLobby();
      break;
    }
    case 'user-online': {
      if (!ws._domain) return;
      const peer = federationPeers[ws._domain];
      if (!peer) { console.warn(`[federation] ← user-online from ${ws._domain} dropped (peer not yet registered)`); return; }
      if (msg.username) {
        peer.users.set(msg.username, { pubKey: msg.pubKey || '' });
        console.log(`[federation] ← user-online @${msg.username}@${ws._domain} (now ${peer.users.size} federated user(s))`);
      }
      broadcastLobby();
      break;
    }
    case 'user-offline': {
      if (!ws._domain) return;
      const peer = federationPeers[ws._domain];
      if (!peer) return;
      peer.users.delete(msg.username);
      console.log(`[federation] ← user-offline @${msg.username}@${ws._domain}`);
      broadcastLobby();
      break;
    }
    case 'dm': {
      // Incoming DM for one of our local users — ciphertext only.
      // The recipient's server (us) stores the recipient's copy; the sender's
      // server stores the sender's copy. We never see plaintext.
      // Paid DMs include payment fields; we verify on-chain against OUR escrow
      // (the destination per recipient's rates post) and disburse from it.
      const { from, to, ciphertext, signature, timestamp,
              textPaid, textMemo, textCurrency, msgId, fromServer } = msg;
      if (!from || !to || !ciphertext) return;
      const recipient = lobbyUsers[to];
      const cur       = textCurrency || 'HBD';
      const paid      = textPaid || 0;

      const deliver = () => {
        if (recipient) {
          io.to(recipient.socketId).emit('lobby-dm', {
            from, ciphertext, signature, timestamp,
            textPaid: paid,
            textCurrency: cur,
            fromServer: fromServer || ws._domain
          });
          console.log(`[fed-text] ← @${from}@${fromServer || ws._domain} → @${to}: delivered to local socket (sid ${recipient.socketId.slice(0,6)}…)`);
        } else {
          console.warn(`[fed-text] ← @${from}@${fromServer || ws._domain} → @${to}: recipient NOT in local lobbyUsers — message stored in chat DB only (will surface on next login via dm-history)`);
        }
        chatStoreDm(from, to, ciphertext, null, signature, timestamp, paid, cur);
        fedSend(ws, { type: 'dm-delivered', from, to, msgId: msgId || null });
      };

      if (paid >= 0.001 && textMemo && msgId) {
        // Re-verify on-chain (caller's server already verified, but trust-but-verify)
        const verifier = (cur !== 'HBD' && cur !== 'HIVE')
          ? verifyHiveEnginePayment(from, ESCROW_ACCOUNT, paid, cur, textMemo)
          : verifyHivePayment(from, ESCROW_ACCOUNT, paid, textMemo);
        verifier.then(async ok => {
          if (!ok) {
            console.warn(`[fed-text] ✗ Payment re-verify failed: @${from} → @${ESCROW_ACCOUNT} ${paid} ${cur} (memo: ${textMemo})`);
            fedSend(ws, { type: 'dm-failed', from, to, msgId, reason: 'payment not on chain' });
            return;
          }

          // v0.16.6 — Recipient-side rate enforcement.
          // Caller's server already validated against the rates post, but a
          // malicious or stale-cache caller server could lie. We are the
          // recipient's home server and the only one fully trusted to enforce
          // the recipient's own policies (block-list, rate, platform fee min).
          // On reject: refund the caller from our escrow.
          //
          // Multi-currency: the picker shows all options the caller qualifies
          // for; the caller picks one and pays in THAT currency. Look up the
          // option matching the actual paid currency, not the resolver's
          // first-pick. (Original v0.16.6 used getRatesForCaller which returns
          // a single applicable, so it wrongly rejected when the caller chose
          // a different currency than the resolver's first-token-match.)
          const recipRates = await fetchRates(to);
          const optResult = await computePaymentOptions(recipRates, from, 'text', new Date());
          let rejectReason = null;
          if (optResult.blocked) {
            rejectReason = optResult.message || 'sender is blocked by recipient';
          } else if (optResult.feeRejected) {
            rejectReason = optResult.message || 'recipient platform fee below this server\'s minimum';
          } else if (optResult.options.length > 0) {
            const opt = optResult.options.find(o => o.currency === cur);
            if (!opt) {
              // Caller paid in a currency the recipient doesn't accept.
              // (E.g. recipient accepts CNOOBS + HBD; caller sent SHITCOIN.)
              // Reject + refund — recipient hasn't agreed to this currency.
              rejectReason = `recipient does not accept ${cur} for paid DMs`;
            } else {
              const requiredRate = opt.flat || 0;
              if (requiredRate >= 0.001 && paid < requiredRate) {
                rejectReason = `underpaid (required ${requiredRate} ${cur}, paid ${paid} ${cur})`;
              }
            }
          }
          // If optResult.options.length === 0: recipient has no rates post
          // OR no matching window right now → free DM territory; let through.
          if (rejectReason) {
            console.warn(`[fed-text] ✗ Recipient-side rate check failed: @${from} → @${to}: ${rejectReason}`);
            const refundMemo = `v4call:text-refund:${msgId}`;
            ledgerPayment(msgId, 'text_refund', ESCROW_ACCOUNT, from, paid, refundMemo, 'pending');
            sendFromEscrow(from, paid, refundMemo, cur, msgId).then(r => {
              if (r.success) {
                ledgerPaymentUpdate(msgId, 'text_refund', 'sent', r.txId);
                console.log(`[fed-text] Refund sent to @${from}: ${paid} ${cur} (tx ${r.txId})`);
              } else {
                ledgerPaymentUpdate(msgId, 'text_refund', 'failed', null);
                console.error(`[fed-text] Refund to @${from} failed: ${r.reason}`);
              }
            });
            fedSend(ws, { type: 'dm-failed', from, to, msgId, reason: rejectReason });
            return;
          }

          ledgerPayment(msgId, 'text', from, ESCROW_ACCOUNT, paid, textMemo, 'verified');

          // Disburse from our escrow — recipient gets net, we keep platform cut.
          const platformCut  = parseFloat((paid * PLATFORM_FEE).toFixed(3));
          const recipientNet = parseFloat((paid - platformCut).toFixed(3));

          if (recipientNet >= 0.001) {
            const payoutMemo = `v4call:text-payout:${msgId}`;
            ledgerPayment(msgId, 'text_payout', ESCROW_ACCOUNT, to, recipientNet, payoutMemo, 'pending');
            sendFromEscrow(to, recipientNet, payoutMemo, cur, msgId).then(r => {
              if (r.success) {
                ledgerPaymentUpdate(msgId, 'text_payout', 'sent', r.txId);
                if (recipient) io.to(recipient.socketId).emit('text-payment-received', {
                  from, amount: recipientNet, currency: cur, msgId
                });
              } else {
                console.error(`[fed-text] Payout to @${to} failed: ${r.reason}`);
              }
            });
          }
          if (platformCut >= 0.001) {
            const feeMemo = `v4call:text-fee:${msgId}`;
            ledgerPayment(msgId, 'text_fee', ESCROW_ACCOUNT, SERVER_HIVE_ACCOUNT, platformCut, feeMemo, 'pending');
            sendFromEscrow(SERVER_HIVE_ACCOUNT, platformCut, feeMemo, cur, msgId).then(r => {
              if (r.success) ledgerPaymentUpdate(msgId, 'text_fee', 'sent', r.txId);
            });
          }

          console.log(`[fed-text] @${from}@${fromServer || ws._domain} → @${to}: paid ${paid} ${cur} | net ${recipientNet} | fee ${platformCut}`);
          deliver();
        });
      } else {
        deliver();
        console.log(`[federation] DM relayed: @${from}@${fromServer || ws._domain} → @${to}`);
      }
      break;
    }
    case 'dm-delivered': {
      // Confirmation from the recipient's server — currently informational only.
      break;
    }
    case 'dm-failed': {
      // Notify the original sender if still online.
      const sender = lobbyUsers[msg.from];
      if (sender) io.to(sender.socketId).emit('lobby-dm-error', `DM to @${msg.to} failed: ${msg.reason || 'unknown'}`);
      break;
    }
    case 'dm-attachment': {
      // v0.16.18 — Incoming ipfs-gate attachment DM for one of our local users.
      // The envelope is ciphertext-key wrapped to {sender, to_user}; we never
      // see plaintext. Paid path: re-verify on-chain to OUR escrow (we're the
      // recipient's home server = treasurer), recipient-side rate enforcement
      // per design rule #15, refund on reject.
      const { from, to, envelope, msgId, textPaid, textMemo, textCurrency, fromServer } = msg;
      if (!from || !to || !envelope || !envelope.cid || !envelope.envelope_sig) return;
      if (!envelope.per_recipient || typeof envelope.per_recipient !== 'object') return;
      if (!(from in envelope.per_recipient) || !(to in envelope.per_recipient)) return;

      const recipient = lobbyUsers[to];
      const cur       = textCurrency || 'HBD';
      const paid      = textPaid || 0;

      const deliver = () => {
        // Persist on our side so recipient's dm-attachments-history replay
        // returns it on next login. The envelope holds both wrapped keys, so
        // either user can decrypt their own copy from the same row.
        chatStoreDmAttachment(envelope, paid, cur);
        if (recipient) io.to(recipient.socketId).emit('dm-attachment', envelope);
      };

      if (paid >= 0.001 && textMemo && msgId) {
        // Re-verify on-chain to OUR escrow (we're the treasurer here).
        const verifier = (cur !== 'HBD' && cur !== 'HIVE')
          ? verifyHiveEnginePayment(from, ESCROW_ACCOUNT, paid, cur, textMemo)
          : verifyHivePayment(from, ESCROW_ACCOUNT, paid, textMemo);
        verifier.then(async ok => {
          if (!ok) {
            console.warn(`[fed-att] ✗ Payment re-verify failed: @${from} → @${ESCROW_ACCOUNT} ${paid} ${cur} (memo: ${textMemo})`);
            fedSend(ws, { type: 'dm-attachment-failed', from, to, msgId, reason: 'payment not on chain' });
            return;
          }

          // Recipient-side rate enforcement (design rule #15).
          const recipRates = await fetchRates(to);
          const optResult  = await computePaymentOptions(recipRates, from, 'text', new Date());
          let rejectReason = null;
          if (optResult.blocked) {
            rejectReason = optResult.message || 'sender is blocked by recipient';
          } else if (optResult.feeRejected) {
            rejectReason = optResult.message || 'recipient platform fee below this server\'s minimum';
          } else if (optResult.options.length > 0) {
            const opt = optResult.options.find(o => o.currency === cur);
            if (!opt) {
              rejectReason = `recipient does not accept ${cur} for paid attachments`;
            } else {
              const requiredRate = opt.flat || 0;
              if (requiredRate >= 0.001 && paid < requiredRate) {
                rejectReason = `underpaid (required ${requiredRate} ${cur}, paid ${paid} ${cur})`;
              }
            }
          }
          if (rejectReason) {
            console.warn(`[fed-att] ✗ Recipient-side check failed: @${from} → @${to}: ${rejectReason}`);
            const refundMemo = `v4call:dm-att-refund:${msgId}`;
            ledgerPayment(msgId, 'text_refund', ESCROW_ACCOUNT, from, paid, refundMemo, 'pending', null, cur);
            sendFromEscrow(from, paid, refundMemo, cur, msgId).then(r => {
              if (r.success) {
                ledgerPaymentUpdate(msgId, 'text_refund', 'sent', r.txId);
                console.log(`[fed-att] Refund sent to @${from}: ${paid} ${cur} (tx ${r.txId})`);
              } else {
                ledgerPaymentUpdate(msgId, 'text_refund', 'failed', null);
                console.error(`[fed-att] Refund to @${from} failed: ${r.reason}`);
              }
            });
            fedSend(ws, { type: 'dm-attachment-failed', from, to, msgId, reason: rejectReason });
            return;
          }

          ledgerPayment(msgId, 'text', from, ESCROW_ACCOUNT, paid, textMemo, 'verified', null, cur);

          // Disburse net to recipient, fee to platform.
          const platformCut  = parseFloat((paid * PLATFORM_FEE).toFixed(3));
          const recipientNet = parseFloat((paid - platformCut).toFixed(3));

          if (recipientNet >= 0.001) {
            const payoutMemo = `v4call:dm-att-payout:${msgId}`;
            ledgerPayment(msgId, 'text_payout', ESCROW_ACCOUNT, to, recipientNet, payoutMemo, 'pending', null, cur);
            sendFromEscrow(to, recipientNet, payoutMemo, cur, msgId).then(r => {
              if (r.success) {
                ledgerPaymentUpdate(msgId, 'text_payout', 'sent', r.txId);
                if (recipient) io.to(recipient.socketId).emit('text-payment-received', {
                  from, amount: recipientNet, currency: cur, msgId
                });
              } else {
                console.error(`[fed-att] Payout to @${to} failed: ${r.reason}`);
              }
            });
          }
          if (platformCut >= 0.001) {
            const feeMemo = `v4call:dm-att-fee:${msgId}`;
            ledgerPayment(msgId, 'text_fee', ESCROW_ACCOUNT, SERVER_HIVE_ACCOUNT, platformCut, feeMemo, 'pending', null, cur);
            sendFromEscrow(SERVER_HIVE_ACCOUNT, platformCut, feeMemo, cur, msgId).then(r => {
              if (r.success) ledgerPaymentUpdate(msgId, 'text_fee', 'sent', r.txId);
            });
          }

          console.log(`[fed-att] @${from}@${fromServer || ws._domain} → @${to}: paid ${paid} ${cur} | net ${recipientNet} | fee ${platformCut}`);
          deliver();
        });
      } else {
        deliver();
        console.log(`[federation] DM attachment relayed: @${from}@${fromServer || ws._domain} → @${to} (cid ${envelope.cid.slice(0,12)}…)`);
      }
      break;
    }
    case 'dm-attachment-failed': {
      // Peer rejected our forwarded attachment. Notify the original sender.
      const sender = lobbyUsers[msg.from];
      if (sender) io.to(sender.socketId).emit('dm-attachment-error', {
        msgId: msg.msgId || null,
        error: `Attachment to @${msg.to} failed: ${msg.reason || 'unknown'}`
      });
      break;
    }
    case 'call-invite': {
      // A peer server is asking us to ring one of our local users.
      const { caller, callee, callType, roomName, callerPubKey, callerServer, ringFeePaid } = msg;
      if (!caller || !callee || !roomName) return;
      const calleeUser = lobbyUsers[callee];
      if (!calleeUser) {
        fedSend(ws, { type: 'call-missed', caller, callee, roomName, reason: 'offline' });
        return;
      }
      if (calleeUser.inCall) {
        fedSend(ws, { type: 'call-declined', caller, callee, roomName, reason: 'busy' });
        return;
      }
      // Mark the callee as in-call so local traffic doesn't collide.
      calleeUser.inCall = roomName;
      // Remember the federated context for this pending invite.
      calleeUser.pendingFederatedCall = { roomName, caller, callerServer };
      io.to(calleeUser.socketId).emit('incoming-call', {
        caller,
        callerPubKey: callerPubKey || '',
        roomName,
        callType: callType || 'voice',
        ringFeePaid: ringFeePaid || 0,
        callerServer: callerServer || ws._domain
      });
      console.log(`[federation] Incoming call: @${caller}@${callerServer || ws._domain} → @${callee} (${callType})`);
      break;
    }
    case 'call-response': {
      // Our local user is the caller; the peer's user has accepted/declined.
      const { caller, callee, accepted, roomName } = msg;
      const callerUser = lobbyUsers[caller];
      const room       = rooms[roomName];
      if (!callerUser) return;
      if (accepted) {
        if (room && room._callTimer) { clearTimeout(room._callTimer); delete room._callTimer; }
        io.to(callerUser.socketId).emit('call-accepted', { callee, roomName });
        if (room) {
          const now = Date.now();
          if (activePayments[room.callId]) activePayments[room.callId].startTime = now;
          ledgerCallUpdate(room.callId, { connected_at: new Date(now).toISOString(), status: 'connected' });
          startCreditBurn(room.callId, roomName);

          const capMs   = MAX_CALL_DURATION_MIN * 60 * 1000;
          const warnMs  = Math.max(0, (MAX_CALL_DURATION_MIN - 5) * 60 * 1000);
          room._capTimer  = setTimeout(async () => {
            if (rooms[roomName]) {
              io.to(roomName).emit('call-cap-reached', { maxMinutes: MAX_CALL_DURATION_MIN });
              await processCallEnd(room.callId, roomName, io, lobbyUsers, 'cap_reached');
            }
          }, capMs);
          room._warnTimer = setTimeout(() => {
            if (rooms[roomName]) io.to(roomName).emit('call-cap-warning', { minutesLeft: 5 });
          }, warnMs);
        }
        console.log(`[federation] ✓ Call accepted: @${caller} ↔ @${callee}`);
      } else {
        io.to(callerUser.socketId).emit('call-declined', { callee, roomName });
        delete callerUser.inCall;
        if (rooms[roomName]) {
          if (rooms[roomName]._callTimer) clearTimeout(rooms[roomName]._callTimer);
          delete rooms[roomName];
        }
        ledgerCallUpdate(msg.roomName, { status: 'declined', end_reason: 'declined', ended_at: new Date().toISOString() });
      }
      break;
    }
    case 'call-declined': {
      const callerUser = lobbyUsers[msg.caller];
      if (callerUser) {
        io.to(callerUser.socketId).emit('call-declined', { callee: msg.callee, roomName: msg.roomName });
        delete callerUser.inCall;
      }
      if (msg.roomName && rooms[msg.roomName]) delete rooms[msg.roomName];
      break;
    }
    case 'call-cancelled': {
      // The remote caller gave up before our user answered.
      const calleeUser = lobbyUsers[msg.callee];
      if (calleeUser) {
        io.to(calleeUser.socketId).emit('call-cancelled', { caller: msg.caller, roomName: msg.roomName });
        delete calleeUser.inCall;
        delete calleeUser.pendingFederatedCall;
      }
      // v0.16.9 — persist cancelled status on our ledger so the callee sees
      // a refund popup. The federation `payment-verified` handler already
      // created the call row + ring payment row on this server.
      if (msg.roomName) {
        ledgerCallUpdate(msg.roomName, {
          status: 'cancelled',
          end_reason: 'cancelled_by_caller',
          ended_at: new Date().toISOString()
        });
      }
      break;
    }
    case 'call-missed': {
      // Our local user was the callee — the peer caller's ring timed out.
      const calleeUser = lobbyUsers[msg.callee];
      if (calleeUser) {
        io.to(calleeUser.socketId).emit('call-missed', { caller: msg.caller, roomName: msg.roomName });
        delete calleeUser.inCall;
        delete calleeUser.pendingFederatedCall;
      }
      // v0.16.9 — persist missed status on our ledger so the callee sees a
      // refund popup. The federation `payment-verified` handler already
      // created the call row + ring payment row on this server.
      if (msg.roomName) {
        ledgerCallUpdate(msg.roomName, {
          status: 'missed',
          end_reason: 'timeout',
          ended_at: new Date().toISOString()
        });
      }
      break;
    }

    case 'payment-verified': {
      // Caller's server forwarded a verified payment. We re-verify on-chain
      // (the payment should have landed in OUR escrow since we host the callee)
      // and record it in activePayments so processFederatedCallEnd can settle.
      const { paymentType, callId, from, to, amount, currency, memo } = msg;
      if (!callId || !from || !to) break;
      const cur = currency || 'HBD';
      const verifier = (cur !== 'HBD' && cur !== 'HIVE')
        ? verifyHiveEnginePayment(from, ESCROW_ACCOUNT, amount, cur, memo)
        : verifyHivePayment(from, ESCROW_ACCOUNT, amount, memo);
      verifier.then(async ok => {
        if (!ok) {
          console.warn(`[fed-payment] ✗ Re-verify failed for ${paymentType} ${callId} (@${from} → @${ESCROW_ACCOUNT} ${amount} ${cur})`);
          fedSend(ws, { type: 'payment-rejected', callId, paymentType, reason: 'on-chain not found' });
          return;
        }

        // v0.16.6 — Recipient-side rate enforcement for paid calls.
        // Same multi-currency logic as the dm handler above: look up the
        // option matching the currency the caller actually paid in, not the
        // resolver's first-pick. Validate ring fee + use OUR computed
        // ratePerHour from THAT currency option (so settlement billing is
        // correct in the currency the call was opened in).
        let computedRatePerHour = null;
        let computedPlatformFee = null;
        if (paymentType === 'ring') {
          const recipRates = await fetchRates(to);
          const optResult = await computePaymentOptions(recipRates, from, msg.callType || 'voice', new Date());
          let rejectReason = null;
          if (optResult.blocked) {
            rejectReason = optResult.message || 'caller is blocked by recipient';
          } else if (optResult.feeRejected) {
            rejectReason = optResult.message || 'recipient platform fee below this server\'s minimum';
          } else if (optResult.options.length > 0) {
            const opt = optResult.options.find(o => o.currency === cur);
            if (!opt) {
              rejectReason = `recipient does not accept ${cur} for paid calls`;
            } else {
              const requiredRing = opt.ring || 0;
              if (requiredRing >= 0.001 && amount < requiredRing) {
                rejectReason = `ring fee underpaid (required ${requiredRing} ${cur}, paid ${amount} ${cur})`;
              }
              computedRatePerHour = opt.rate;
              computedPlatformFee = opt.platformFee;
            }
          }
          if (rejectReason) {
            console.warn(`[fed-payment] ✗ Recipient-side rate check failed: ${callId} ring — ${rejectReason}`);
            const refundMemo = `v4call:ring-refund:${callId}`;
            ledgerPayment(callId, 'ring_refund', ESCROW_ACCOUNT, from, amount, refundMemo, 'pending');
            sendFromEscrow(from, amount, refundMemo, cur, callId).then(r => {
              if (r.success) {
                ledgerPaymentUpdate(callId, 'ring_refund', 'sent', r.txId);
                console.log(`[fed-payment] Ring refund sent to @${from}: ${amount} ${cur} (tx ${r.txId})`);
              } else {
                ledgerPaymentUpdate(callId, 'ring_refund', 'failed', null);
                console.error(`[fed-payment] Ring refund to @${from} failed: ${r.reason}`);
              }
            });
            fedSend(ws, { type: 'payment-rejected', callId, paymentType, reason: rejectReason });
            return;
          }
        }

        if (!activePayments[callId]) activePayments[callId] = {};
        const p = activePayments[callId];
        p.caller        = from;
        p.callee        = to;
        p.currency      = cur;
        p.callerServer  = msg.callerServer || ws._domain;
        if (!p.startTime) p.startTime = Date.now();

        if (paymentType === 'ring') {
          p.ringPaid    = amount;
          p.ringMemo    = memo;
          // v0.16.6: prefer OUR computed rate-per-hour over the caller's claim.
          // computedRatePerHour comes from our own fetchRates(to) above; falls
          // back to msg.ratePerHour only if our rates fetch returned nothing.
          p.ratePerHour      = (computedRatePerHour !== null && computedRatePerHour !== undefined)
            ? computedRatePerHour : msg.ratePerHour;
          p.posterPlatformFee = (computedPlatformFee !== null && computedPlatformFee !== undefined)
            ? computedPlatformFee : msg.platformFee;
          if (msg.callType    !== undefined) p.callType    = msg.callType;
          ledgerCallCreate(callId, from, to, msg.callType || 'voice');
          // v0.16.9 — pass currency so missed-call popup shows the actual token
          ledgerPayment(callId, 'ring', from, ESCROW_ACCOUNT, amount, memo, 'verified', null, cur);
          // Also mark this call as 'ringing' so the missed/cancelled status
          // updates from federation `call-missed` / `call-cancelled` find a row.
          ledgerCallUpdate(callId, { status: 'ringing' });
        } else if (paymentType === 'deposit') {
          p.depositPaid     = msg.depositAmount || amount;
          p.connectPaid     = msg.connectAmount || 0;
          p.creditRemaining = p.depositPaid;
          ledgerPayment(callId, 'deposit', from, ESCROW_ACCOUNT, amount, memo, 'verified', null, cur);
        } else if (paymentType === 'topup') {
          p.depositPaid     = (p.depositPaid     || 0) + amount;
          p.creditRemaining = (p.creditRemaining || 0) + amount;
          ledgerPayment(callId, 'topup', from, ESCROW_ACCOUNT, amount, memo, 'verified', null, cur);
          // Let the local callee's UI know the caller added more credit.
          const calleeSid = lobbyUsers[to]?.socketId;
          if (calleeSid) io.to(calleeSid).emit('credit-topup', {
            amount, creditRemaining: p.creditRemaining, minutesLeft: null
          });
        }
        console.log(`[fed-payment] Recorded ${paymentType} for ${callId}: @${from} paid ${amount} ${cur}`);
      });
      break;
    }

    case 'payment-rejected': {
      console.warn(`[fed-payment] Peer rejected our payment forward: ${msg.callId} ${msg.paymentType} — ${msg.reason}`);
      break;
    }

    case 'call-ended': {
      // Caller's server signalling end of a federated call — we disburse.
      const { callId, durationMs, endReason, callerServer } = msg;
      processFederatedCallEnd(callId, durationMs || 0, endReason || 'unknown', callerServer || ws._domain)
        .catch(e => console.error(`[fed-billing] processFederatedCallEnd failed: ${e.message}`));
      break;
    }

    case 'call-receipt-fed': {
      // Callee's server sent us the caller's receipt after disbursing.
      const { callId, receipt } = msg;
      if (!receipt) break;
      const caller = receipt.caller;
      const callerSid = lobbyUsers[caller]?.socketId;
      if (callerSid) io.to(callerSid).emit('call-receipt', receipt);
      console.log(`[fed-billing] Forwarded caller receipt for ${callId} to @${caller}`);
      break;
    }

    // ── v0.4 — Cross-server room invites ────────────────────────────────────
    case 'room-invite': {
      if (!ws._domain) return;
      const { invite_id, from_user, to_user, room_name, source_server, payload } = msg;
      if (!invite_id || !from_user || !to_user || !room_name || !source_server) {
        console.warn(`[federation] ← room-invite from ${ws._domain} dropped — missing required fields`);
        return;
      }
      if (source_server.toLowerCase() !== ws._domain) {
        console.warn(`[federation] ← room-invite source_server ${source_server} ≠ wire peer ${ws._domain} — dropped`);
        return;
      }
      const target = (to_user || '').toLowerCase();
      const lu = lobbyUsers[target];
      if (!lu) {
        // Target offline — auto-decline so the inviting admin gets feedback.
        // Paid invite payment is auto-refunded by the source server when it
        // receives this 'declined' (see room-response handler with reason='offline').
        fedSend(ws, { type: 'room-response', invite_id, response: 'declined', reason: 'offline' });
        console.log(`[federation] ← room-invite for offline @${target} from @${from_user}@${ws._domain} — auto-declined`);
        return;
      }

      // v0.16.14 — recipient-side fee gate (closes the paid-invite bypass
      // class). Per design rule #15 the recipient is the only fully trusted
      // enforcer of its user's policy. ALWAYS re-fetch the target's invite
      // options here regardless of what the source claims — never trust the
      // source to have charged correctly. Mirrors the v0.16.6 paid-DM
      // hardening: ALL paid flows must do recipient-side enforcement.
      const recipInv = await getInviteOptions(target, from_user);
      if (recipInv.blocked) {
        fedSend(ws, { type: 'room-response', invite_id, response: 'declined', reason: 'paid_rejected', detail: 'blocked' });
        console.log(`[federation] ← room-invite @${from_user}@${ws._domain} → @${target} — recipient blocks inviter — rejected`);
        return;
      }
      if (recipInv.feeRejected) {
        fedSend(ws, { type: 'room-response', invite_id, response: 'declined', reason: 'paid_rejected', detail: 'fee_rejected' });
        console.log(`[federation] ← room-invite @${from_user}@${ws._domain} → @${target} — recipient platform fee mismatch — rejected`);
        return;
      }
      const recipientRequiresPayment = (recipInv.options || []).length > 0;

      // v0.16.11 — recipient-side paid-invite enforcement (design rule #15).
      // If the source server attached a payment, re-validate the offer
      // (rate match in OUR copy of target's rates) and re-verify the on-chain
      // payment landed in the source server's claimed escrow. On any failure
      // send back 'declined' with reason 'paid_rejected' so the source server
      // refunds the inviter from its own escrow.
      const payment = (payload && payload.payment) || null;

      // v0.16.14 — the actual bypass close: target has a paid rate but the
      // source server didn't include payment. Refuse the invite and tell the
      // source what we charge (with our escrow as the destination) so it can
      // pop the picker for the inviter and re-invite with payment. THIS is
      // the line of code that closes the bypass surfaced by Phase D making
      // cross-server invites a daily workflow.
      if (recipientRequiresPayment && !payment) {
        const required = recipInv.options.map(o => ({
          currency: o.currency,
          flat:     o.flat
        }));
        fedSend(ws, {
          type:        'room-response',
          invite_id,
          response:    'declined',
          reason:      'paid_rejected',
          detail:      'fee_required',
          required,                              // [{currency, flat}, ...]
          recipient_escrow: ESCROW_ACCOUNT,      // informational; inviter still pays into its OWN escrow (inviter-holds-funds model)
        });
        console.log(`[federation] ← room-invite @${from_user}@${ws._domain} → @${target} — fee_required (options: ${required.map(o=>`${o.flat} ${o.currency}`).join(' or ')}) — rejected`);
        return;
      }

      if (payment) {
        const { currency, paid, memo, source_escrow } = payment;
        if (!currency || !(paid > 0) || !memo || !source_escrow) {
          fedSend(ws, { type: 'room-response', invite_id, response: 'declined', reason: 'paid_rejected', detail: 'malformed payment payload' });
          console.log(`[federation] ← room-invite paid but malformed payment payload — rejected`);
          return;
        }
        // Source escrow must match this peer's announced escrow (sanity check —
        // we wouldn't accept a paid invite where the source server claims
        // a different account holds the funds than the one in their signed
        // v4call-server.json / hello envelope).
        const peer = federationPeers[ws._domain];
        const declaredEscrow = peer && peer.escrow;
        if (declaredEscrow && source_escrow.toLowerCase() !== declaredEscrow.toLowerCase()) {
          fedSend(ws, { type: 'room-response', invite_id, response: 'declined', reason: 'paid_rejected', detail: `source_escrow ${source_escrow} ≠ peer escrow ${declaredEscrow}` });
          console.log(`[federation] ← room-invite paid but source_escrow ${source_escrow} doesn't match peer's announced @${declaredEscrow} — rejected`);
          return;
        }

        // Re-validate rate: target's rates post must offer an invite option
        // matching the currency the source claims to have charged, at or
        // below the amount paid. Mirrors v0.16.6 paid-DM enforcement.
        // v0.16.14 — reuse recipInv (already fetched + computed above; same
        // 5-min cache, so this is just a property read).
        const matched = (recipInv.options || []).find(o => o.currency === currency);
        if (!matched) {
          fedSend(ws, { type: 'room-response', invite_id, response: 'declined', reason: 'paid_rejected', detail: `currency ${currency} not accepted` });
          console.log(`[federation] ← currency ${currency} not in @${target}'s invite options — rejected`);
          return;
        }
        if (paid + 1e-9 < matched.flat) {
          fedSend(ws, { type: 'room-response', invite_id, response: 'declined', reason: 'paid_rejected', detail: `underpaid: required ${matched.flat} ${currency}, paid ${paid}` });
          console.log(`[federation] ← underpaid for @${target} — rejected`);
          return;
        }

        // Re-verify the on-chain payment is from from_user to source_escrow
        // for the claimed amount + currency + memo. This catches a malicious
        // source server claiming a payment that didn't actually happen.
        const ok = (currency !== 'HBD' && currency !== 'HIVE')
          ? await verifyHiveEnginePayment(from_user, source_escrow, paid, currency, memo)
          : await verifyHivePayment(from_user, source_escrow, paid, memo);
        if (!ok) {
          fedSend(ws, { type: 'room-response', invite_id, response: 'declined', reason: 'paid_rejected', detail: 'on-chain payment not found' });
          console.log(`[federation] ← payment ${paid} ${currency} from @${from_user} → @${source_escrow} (memo ${memo}) not found on chain — rejected`);
          return;
        }
        console.log(`[paid-invite][fed-recv] ✓ Validated paid invite ${invite_id} — @${from_user}@${ws._domain} paid ${paid} ${currency} to @${source_escrow} for @${target}`);
      }

      pendingFederatedInvites[invite_id] = {
        dir:         'incoming',
        target_user: target,
        from_user,
        from_server: ws._domain,
        room:        room_name,
        payload:     payload || {},
        created_at:  Date.now(),
        paid_invite: !!payment
      };
      // Deliver popup with paid badge if applicable so the invitee sees they're
      // being paid (and that declining triggers a refund to the inviter).
      io.to(lu.socketId).emit('room-invite', {
        roomName:       room_name,
        from:           from_user,
        invitees:       [from_user, target],
        from_server:    ws._domain,
        invite_id,
        paid_invite_id: payment ? invite_id : null,
        paid_amount:    payment ? payment.paid : 0,
        paid_currency:  payment ? payment.currency : null
      });
      console.log(`[federation] ← room-invite @${from_user}@${ws._domain} → @${target} #${room_name} (id ${invite_id})${payment ? ` [paid ${payment.paid} ${payment.currency}]` : ''}`);
      break;
    }

    case 'room-response': {
      if (!ws._domain) return;
      const { invite_id, response, reason, detail } = msg;
      const entry = pendingFederatedInvites[invite_id];
      if (!entry || entry.dir !== 'outgoing') {
        console.warn(`[federation] ← room-response for unknown invite_id ${invite_id} from ${ws._domain}`);
        return;
      }
      if (entry.target_server !== ws._domain) {
        console.warn(`[federation] ← room-response from ${ws._domain} but invite was for ${entry.target_server} — dropped`);
        return;
      }
      const wasPaid = !!entry.paid_invite;
      delete pendingFederatedInvites[invite_id];
      console.log(`[federation] ← room-response ${response} for #${entry.room} (@${entry.target_user}@${entry.target_server})${reason ? ' — ' + reason : ''}${detail ? ' (' + detail + ')' : ''}`);

      const adminLu = lobbyUsers[entry.from_user];

      // v0.16.11 — settle paid federated invite. On accept we disburse from
      // our escrow (net to invitee via cross-chain transfer + fee to our
      // operator). On decline (whether user-decline, offline auto-decline,
      // or recipient-side paid_rejected) we refund the inviter.
      if (wasPaid && pendingPaidInvites[invite_id]) {
        if (response === 'accepted') {
          disbursePaidInvite(invite_id);
        } else {
          // Roll back the canonical allowlist entry too — if recipient declined
          // or rejected, the invitee shouldn't sit on the allowlist any more.
          const r = rooms[entry.room];
          if (r) {
            const canonical = `${entry.target_user}@${entry.target_server}`;
            r.allowlist.delete(canonical);
            emitRoomInfoToMembers(r, entry.room);
            broadcastRooms();
          }
          const refundReason = (reason === 'paid_rejected') ? 'rejected_by_peer'
                             : (reason === 'offline')      ? 'offline'
                             :                               'declined';
          refundPaidInvite(invite_id, refundReason);
          if (reason === 'paid_rejected' && adminLu) {
            io.to(adminLu.socketId).emit('lobby-info', {
              text: `⚠ ${entry.target_server} rejected your paid invite to @${entry.target_user}: ${detail || 'no detail'}. Refund in flight.`
            });
          }
        }
        break; // refund/disburse paths handle their own admin notifications
      }

      // v0.16.14 — recipient told us we owe a fee for this user. Pop the
      // payment picker on the inviter's client so they can pay and re-invite.
      // Also roll back the optimistic allowlist entry we added at free-send
      // time. The picker will trigger a new allowlist-add with payment fields,
      // which the source-side handler now routes through the paid path even
      // when source-side rates think the invitee is free (cache stale, etc.).
      if (response === 'declined' && reason === 'paid_rejected' && detail === 'fee_required') {
        const r = rooms[entry.room];
        if (r) {
          const canonical = `${entry.target_user}@${entry.target_server}`;
          r.allowlist.delete(canonical);
          emitRoomInfoToMembers(r, entry.room);
          broadcastRooms();
        }
        if (adminLu) {
          const required = Array.isArray(msg.required) ? msg.required : [];
          if (required.length > 0) {
            const newInviteId = crypto.randomBytes(12).toString('hex');
            io.to(adminLu.socketId).emit('invite-payment-required', {
              room:       entry.room,
              invitee:    `${entry.target_user}@${entry.target_server}`,
              inviteId:   newInviteId,
              escrow:     ESCROW_ACCOUNT,         // inviter-holds-funds — they pay into OUR escrow
              options:    required.map(o => ({ currency: o.currency, flat: o.flat, balance: null })),
              belowFloor: false,
              federated:  true,
              peerServer: entry.target_server,
              fromFeeRequired: true,              // hint for client UX text
            });
          } else {
            io.to(adminLu.socketId).emit('lobby-info', {
              text: `⚠ @${entry.target_user}@${entry.target_server} requires payment but the recipient server didn't send the rate options. Try again shortly.`
            });
          }
        }
        break;
      }

      // v0.16.14 — recipient says the inviter is on the target's block list.
      // Surface clearly and roll back the optimistic allowlist add.
      if (response === 'declined' && reason === 'paid_rejected' && detail === 'blocked') {
        const r = rooms[entry.room];
        if (r) {
          const canonical = `${entry.target_user}@${entry.target_server}`;
          r.allowlist.delete(canonical);
          emitRoomInfoToMembers(r, entry.room);
          broadcastRooms();
        }
        if (adminLu) {
          io.to(adminLu.socketId).emit('lobby-info', {
            text: `⚠ @${entry.target_user}@${entry.target_server} has blocked you — invite refused.`
          });
        }
        break;
      }

      if (response === 'accepted' && adminLu) {
        io.to(adminLu.socketId).emit('lobby-info', {
          text: `✓ @${entry.target_user}@${entry.target_server} accepted invite to #${entry.room}.`
        });
      } else if (response === 'declined' && reason === 'offline' && adminLu) {
        // Offline auto-decline — surface so admin knows it didn't reach the user.
        io.to(adminLu.socketId).emit('lobby-info', {
          text: `⚠ @${entry.target_user}@${entry.target_server} is offline — invite to #${entry.room} not delivered.`
        });
      }
      // Explicit user decline is silent on the inviter side (matches local invite behaviour).
      break;
    }
  }
}

// Periodic prune of pending federated invites — drops any that have been
// outstanding longer than FED_INVITE_TTL_MS (e.g. peer disconnected mid-flow,
// user never responded). Logs each expiry so debugging mid-flight drops is easy.
setInterval(() => {
  const cutoff = Date.now() - FED_INVITE_TTL_MS;
  for (const [id, entry] of Object.entries(pendingFederatedInvites)) {
    if (entry.created_at < cutoff) {
      console.log(`[federation] Pending invite ${id} (${entry.dir}) expired after ${Math.round((Date.now() - entry.created_at) / 60000)}m`);
      // v0.16.11 — also trigger a paid-invite refund if this fed invite was
      // paid. The 60s paid-invite sweep would catch it eventually, but doing
      // it here keeps the two maps in lockstep and rolls back the allowlist
      // entry that the source-side allowlist-add added at payment time.
      if (entry.paid_invite && pendingPaidInvites[id] && pendingPaidInvites[id].status === 'pending') {
        const r = rooms[entry.room];
        if (r && entry.target_server) {
          const canonical = `${entry.target_user}@${entry.target_server}`;
          r.allowlist.delete(canonical);
          emitRoomInfoToMembers(r, entry.room);
          broadcastRooms();
        }
        refundPaidInvite(id, 'timed_out');
      }
      delete pendingFederatedInvites[id];
    }
  }
}, 5 * 60 * 1000).unref?.();

// v0.16.10 — TTL sweep for paid LOCAL invites. Anything still in 'pending'
// past PAID_INVITE_TTL_MS gets auto-refunded to the inviter and the entry
// marked 'timed_out' (kept briefly so a late accept can detect "this was
// already refunded").
setInterval(() => {
  const cutoff = Date.now() - PAID_INVITE_TTL_MS;
  for (const [id, entry] of Object.entries(pendingPaidInvites)) {
    if (entry.status === 'pending' && entry.created_at < cutoff) {
      console.log(`[paid-invite] ${id} timed out after ${Math.round((Date.now()-entry.created_at)/60000)}m — refunding @${entry.inviter}`);
      // For federated entries, also roll back the canonical allowlist entry
      // before refunding (mirrors the federated room-response decline path).
      if (entry.federated && entry.invitee_server) {
        const r = rooms[entry.room];
        if (r) {
          const canonical = `${entry.invitee}@${entry.invitee_server}`;
          r.allowlist.delete(canonical);
          emitRoomInfoToMembers(r, entry.room);
          broadcastRooms();
        }
      }
      refundPaidInvite(id, 'timed_out');
    } else if (entry.status !== 'pending' && entry.created_at < (Date.now() - 30 * 60 * 1000)) {
      // Drop completed entries after 30m to keep the map tidy.
      delete pendingPaidInvites[id];
    }
  }
}, 60 * 1000).unref?.();

// Refunds the inviter the gross amount they paid, marks the entry, and
// notifies the inviter if they're online. Idempotent — re-entry is safe.
async function refundPaidInvite(inviteId, newStatus) {
  const e = pendingPaidInvites[inviteId];
  if (!e || e.status !== 'pending') return;
  e.status = newStatus; // mark first so concurrent accept/decline see it locked
  const refundMemo = `v4call:invite-refund:${inviteId}`;
  try {
    const r = await sendFromEscrow(e.inviter, e.paid, refundMemo, e.currency, inviteId);
    if (r && r.success) {
      ledgerPayment(inviteId, 'invite_refund', ESCROW_ACCOUNT, e.inviter, e.paid, refundMemo, 'sent', r.txId || null, e.currency);
      const lu = lobbyUsers[e.inviter];
      const inviteeLabel = e.invitee_server ? `@${e.invitee}@${e.invitee_server}` : `@${e.invitee}`;
      if (lu) io.to(lu.socketId).emit('lobby-info', {
        text: `↩️ Invite to ${inviteeLabel} (#${e.room}) — ${newStatus.replace('_', ' ')}. Refunded ${e.paid.toFixed(3)} ${e.currency}.`
      });
      console.log(`[paid-invite] ↩ Refunded ${e.paid} ${e.currency} to @${e.inviter} for ${inviteId} (${newStatus})`);
    } else {
      console.error(`[paid-invite] ✗ Refund failed for ${inviteId}: ${r && r.reason}`);
      ledgerPayment(inviteId, 'invite_refund', ESCROW_ACCOUNT, e.inviter, e.paid, refundMemo, 'failed', null, e.currency);
    }
  } catch (err) {
    console.error(`[paid-invite] Refund error for ${inviteId}:`, err.message);
  }
}

function fedAttachSocket(ws, label) {
  // fedHandleMessage is async (hello awaits verifyPeer). We chain handlers in
  // a per-socket Promise queue so messages are processed STRICTLY in order —
  // a presence/user-online arriving while hello's verification is still in
  // flight waits for hello to finish registering the peer, instead of being
  // silently dropped by "federationPeers[ws._domain] not yet set".
  ws._processQueue = Promise.resolve();
  ws.on('message', (data) => {
    ws._processQueue = ws._processQueue
      .then(() => fedHandleMessage(ws, data.toString()))
      .catch(e => console.error('[federation] handler error:', e.message));
  });
  ws.on('close',   () => {
    if (ws._domain && federationPeers[ws._domain]?.ws === ws) {
      const domain = ws._domain;
      federationPeers[domain].connected = false;
      console.log(`[federation] ${label} to ${domain} closed`);
      cleanupFederatedMembersForPeer(domain); // v0.16 Part B
      broadcastLobby();
    }
  });
  ws.on('error', (e) => {
    console.warn(`[federation] ${label} error (${ws._domain || 'unidentified'}): ${e.message}`);
  });
  // Say hello immediately. Include our ESCROW_ACCOUNT so peers can detect
  // rates-post escrow mismatches, and our SERVER_HIVE_ACCOUNT so peers can
  // pin their v4call-server.json check to the correct signer.
  fedSend(ws, {
    type:             'hello',
    domain:           SERVER_DOMAIN,
    name:             SERVER_NAME,
    version:          FEDERATION_VERSION,
    protocol_version: FEDERATION_VERSION, // explicit gate field for v0.4+ features (cross-server room invites etc.)
    escrow:           ESCROW_ACCOUNT,
    hive_account:     SERVER_HIVE_ACCOUNT
  });
}

// Inbound WebSocket server (peers connect to us at /federation)
const federationWss = new WebSocket.Server({ noServer: true });
federationWss.on('connection', (ws) => {
  ws._isOutbound = false;
  console.log('[federation] Inbound peer connection — waiting for hello');
  fedAttachSocket(ws, 'inbound');
});

server.on('upgrade', (request, socket, head) => {
  const url = request.url || '';
  if (url === '/federation' || url.startsWith('/federation?')) {
    federationWss.handleUpgrade(request, socket, head, (ws) => {
      federationWss.emit('connection', ws, request);
    });
  }
  // All other upgrades (including /socket.io/) are handled by Socket.io directly.
});

// Outbound connections to configured peers, with exponential-backoff reconnect.
// Only the lexicographically-smaller domain initiates outbound — the other side
// just accepts inbound. Both servers can keep FEDERATION_PEERS set; the higher-
// domain side simply skips its outbound attempts (passive mode).
function fedConnectPeer(url) {
  let peerHost;
  try { peerHost = new URL(url).host.toLowerCase(); }
  catch(e) { console.warn(`[federation] Bad peer URL: ${url}`); return; }

  if (!fedShouldInitiate(peerHost)) {
    console.log(`[federation] Passive mode for ${peerHost} (domain tiebreaker — peer will initiate)`);
    return;
  }

  let attempt = 0;
  const open = () => {
    // If a healthy connection (e.g. inbound from the same peer) already exists,
    // hold off and check again later — saves a connect/reject round trip.
    const existing = federationPeers[peerHost];
    if (existing && existing.ws && existing.ws.readyState === WebSocket.OPEN) {
      setTimeout(open, 30000);
      return;
    }
    console.log(`[federation] Connecting to ${url}...`);
    const ws = new WebSocket(url);
    ws._isOutbound = true;
    ws.on('open', () => {
      attempt = 0;
      console.log(`[federation] Outbound connected: ${url}`);
      fedAttachSocket(ws, 'outbound');
      fedSendLocalPresence(ws);
    });
    ws.on('close', () => {
      attempt++;
      const delay = Math.min(1000 * Math.pow(2, Math.min(attempt, 6)), 60000);
      console.log(`[federation] Disconnected from ${url} — retry in ${Math.round(delay/1000)}s`);
      setTimeout(open, delay);
    });
    ws.on('error', () => {
      // 'close' will fire after 'error' — reconnect is handled there.
    });
  };
  open();
}

// v0.16.15 — persisted approvals always load, regardless of whether .env
// has seed peers. Without this, commenting out FEDERATION_PEERS in .env
// silently dropped every previously-approved peer from in-memory state on
// next boot (the JSON file was untouched but never read). Now: disk wins,
// matching the data/ bind-mount pattern everywhere else in the codebase.
// The .env seeding inside loadApprovedPeers is a no-op when FEDERATION_PEERS
// is empty, so this is safe to run unconditionally.
loadApprovedPeers();
console.log(`[federation] Approved peers: ${[...approvedPeers].join(', ') || '(none)'}`);

if (FEDERATION_ENABLED) {
  for (const url of FEDERATION_PEERS) fedConnectPeer(url);
  // Kick off discovery shortly after startup (non-blocking), then every 2 hours.
  // Gated by HIVE_SCAN_ENABLED so FED_DISCOVERY_MODE=nostr + NOSTR_HIVE_FALLBACK=false
  // can run pure-Nostr-no-fallback test mode.
  if (HIVE_SCAN_ENABLED) {
    setTimeout(() => {
      scanV4CallDirectory().catch(e => console.error('[discovery] scan error:', e.message));
    }, 5000);
    setInterval(() => {
      scanV4CallDirectory().catch(e => console.error('[discovery] scan error:', e.message));
    }, 2 * 60 * 60 * 1000);
  } else {
    console.log('[discovery] Hive 2h scan DISABLED (FED_DISCOVERY_MODE=nostr, NOSTR_HIVE_FALLBACK=false) — Nostr is the only discovery channel');
  }
}

// ── Nostr federation (Phase B — publish own announce; non-blocking) ─────────
// Loaded via dynamic import() because nostr-tools is ESM-only and server.js is
// CommonJS. Fire-and-forget: any failure is logged inside and never throws,
// so v4call runs normally regardless of Nostr state. Loaded in ALL modes
// (even 'hive') so the identity key + npub are generated and printed for the
// operator to pre-stage in their Hive announce; the module itself skips
// publishing when mode is 'hive'.
{
  import('./nostr-fed.mjs').then(({ startNostrFed }) => {
    return startNostrFed({
      domain:        SERVER_DOMAIN,
      hiveAccount:   SERVER_HIVE_ACCOUNT,
      verifyUrl:     `https://${SERVER_DOMAIN}/.well-known/v4call-server.json`,
      protocol:      FEDERATION_VERSION,
      mode:          FED_DISCOVERY_MODE,
      relays:        NOSTR_RELAYS,
      republishHours: NOSTR_REPUBLISH_HOURS,
      nsecSeed:      NOSTR_NSEC,
      keyPath:       NOSTR_KEY_PATH,
      // Phase C — subscribe + discovery
      subscribeEnabled: NOSTR_SUBSCRIBE_ENABLED,
      onDiscover:    discoverPeerViaNostr,        // module calls us; we own the trust check
      ownDomain:     SERVER_DOMAIN,                // skip events from our own domain
      // Phase D — presence (WS-wins-Nostr-additive)
      presenceEnabled:  FED_PRESENCE_VIA_NOSTR && NOSTR_SUBSCRIBE_ENABLED,
      presenceThrottleMs:  NOSTR_PRESENCE_THROTTLE_SECONDS  * 1000,
      presenceHeartbeatMs: NOSTR_PRESENCE_HEARTBEAT_SECONDS * 1000,
      getLocalUsers:    getLocalOnlineUsernamesForPresence,
      onPresence:       recordNostrPresence,
    });
  }).then(controller => {
    // Module returns a controller on success (presence publish trigger etc.).
    // Stored globally so broadcastLobby can call notePresenceChange().
    if (controller) {
      nostrFedController = controller;
      if (FED_PRESENCE_VIA_NOSTR) console.log('[presence] Phase D enabled — Nostr presence is additive to WS federation');
    }
  }).catch(e => console.error('[nostr] module load failed (v4call continues):', e.message));
}


// ─────────────────────────────────────────────────────────────────────────────
// ── Startup ───────────────────────────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────

server.listen(PORT, BIND_HOST, () => {
  console.log(`\nv4call server running on ${BIND_HOST}:${PORT}`);
  console.log(`Rate format: V1 and V2 supported\n`);

  const escrowKey = process.env.V4CALL_ESCROW_KEY;
  const adminKey  = process.env.ADMIN_KEY;

  if (!escrowKey) {
    console.error('⚠️  WARNING: V4CALL_ESCROW_KEY not set — escrow payouts will not work!');
  } else {
    try {
      const key    = dhive.PrivateKey.fromString(escrowKey);
      const pubKey = key.createPublic().toString();
      console.log(`✓ Escrow key loaded — public: ${pubKey}`);

      getEscrowBalance().then(balance => {
        console.log(`✓ Escrow @${ESCROW_ACCOUNT} balance: ${balance.toFixed(3)} HBD`);
      }).catch(e => {
        console.error(`⚠️  Could not check escrow balance: ${e.message}`);
      });

      hivePost({ jsonrpc: '2.0', method: 'condenser_api.get_accounts', params: [[ESCROW_ACCOUNT]], id: 1 })
        .then(data => {
          if (!data?.result?.[0]) {
            console.error(`⚠️  Could not find @${ESCROW_ACCOUNT} on Hive`);
            return;
          }
          const activeKeys = data.result[0].active?.key_auths?.map(k => k[0]) || [];
          if (activeKeys.includes(pubKey)) {
            console.log(`✓ Escrow key verified against @${ESCROW_ACCOUNT} active key`);
          } else {
            console.error(`⚠️  Escrow key does NOT match @${ESCROW_ACCOUNT} active key!`);
            console.error(`   Derived: ${pubKey}`);
            console.error(`   Expected one of: ${activeKeys.join(', ')}`);
          }
        }).catch(e => {
          console.error(`⚠️  Could not verify escrow key: ${e.message}`);
        });
    } catch(e) {
      console.error(`⚠️  Invalid V4CALL_ESCROW_KEY: ${e.message}`);
    }
  }

  if (!adminKey) {
    console.warn('⚠️  ADMIN_KEY not set — /admin/* endpoints are disabled');
  } else {
    console.log('✓ Admin endpoints available: /admin/ledger  /admin/balance');
  }
});
