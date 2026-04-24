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
const PORT                = parseInt(process.env.PORT        || '3000');
const BIND_HOST           = process.env.BIND_HOST            || '127.0.0.1';

// Hive API nodes — can override the primary node via HIVE_API env var.
// Server tries each in order and falls back automatically on failure.
const HIVE_API       = process.env.HIVE_API || 'https://api.hive.blog';
const HIVE_API_NODES = [
  HIVE_API,
  'https://anyx.io',
  'https://api.deathwing.me',
  'https://hived.emre.sh'
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
const FEDERATION_VERSION = '0.2';

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

  CREATE INDEX IF NOT EXISTS idx_dm_owner      ON dm_messages(owner);
  CREATE INDEX IF NOT EXISTS idx_dm_from       ON dm_messages(from_user);
  CREATE INDEX IF NOT EXISTS idx_dm_to         ON dm_messages(to_user);
  CREATE INDEX IF NOT EXISTS idx_dm_created     ON dm_messages(created_at);
  CREATE INDEX IF NOT EXISTS idx_room_name     ON room_messages(room_name);
  CREATE INDEX IF NOT EXISTS idx_room_created  ON room_messages(created_at);
`);

console.log('[chat] SQLite ready:', path.join(LOG_DIR, 'v4call-chat.db'));

// ── Chat DB helpers ──────────────────────────────────────────────────────────

function chatStoreDm(fromUser, toUser, ciphertextForRecipient, ciphertextForSender, signature, timestamp, textPaid) {
  try {
    const stmt = chatDb.prepare(`
      INSERT INTO dm_messages (from_user, to_user, owner, ciphertext, signature, timestamp, text_paid)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `);
    // Store recipient's copy (encrypted to recipient's key)
    stmt.run(fromUser, toUser, toUser, ciphertextForRecipient, signature, timestamp, textPaid || 0);
    // Store sender's copy (encrypted to sender's key)
    if (ciphertextForSender) {
      stmt.run(fromUser, toUser, fromUser, ciphertextForSender, signature, timestamp, textPaid || 0);
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
      SELECT from_user, to_user, ciphertext, signature, timestamp, text_paid, created_at
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
      SELECT from_user, to_user, ciphertext, signature, timestamp, text_paid, created_at
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
  } catch(e) {
    console.error('[chat] Room delete failed:', e.message);
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
    const dmDel   = chatDb.prepare(`DELETE FROM dm_messages WHERE created_at < ?`).run(dmCutoff);
    const roomDel = chatDb.prepare(`DELETE FROM room_messages WHERE created_at < ?`).run(roomCutoff);
    if (dmDel.changes || roomDel.changes) {
      console.log(`[chat] Cleanup: removed ${dmDel.changes} DMs, ${roomDel.changes} room messages`);
    }
  } catch(e) {
    console.error('[chat] Cleanup failed:', e.message);
  }
}

// Run cleanup on startup and then every hour
chatCleanup();
setInterval(chatCleanup, 60 * 60 * 1000);

// ── Ledger helpers ────────────────────────────────────────────────────────────

function ledgerPayment(callId, type, fromUser, toUser, amount, memo = '', status = 'pending', txId = null) {
  try {
    db.prepare(`
      INSERT INTO payments (call_id, type, from_user, to_user, amount, memo, status, tx_id)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(callId, type, fromUser, toUser, amount, memo, status, txId);
  } catch(e) {
    console.error('[ledger] Payment insert failed:', e.message);
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
const rooms      = {}; // roomName → { creator, allowlist(Set), members[], isCall?, callId?, ... }

// Federation state — populated by federation message handlers (see below).
// domain → { ws, connected, name, users: Map(username → { pubKey }) }
const federationPeers = {};

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
  return [...local, ...federatedUserSnapshot()];
}
function broadcastLobby() { io.emit('lobby-users', lobbySnapshot()); }

// Look up which federation peer hosts a given username. Returns the peer
// record (with ws) or null if no federated peer has that user.
function peerForUser(username) {
  for (const [domain, peer] of Object.entries(federationPeers)) {
    if (!peer.connected) continue;
    if (peer.users.has(username)) return { domain, ...peer };
  }
  return null;
}

function roomsSnapshot() {
  return Object.entries(rooms).map(([name, r]) => ({
    name,
    creator:     r.creator,
    memberCount: r.members.length,
    isCall:      r.isCall || false,
    allowlist:   [...r.allowlist].map(username => ({
      username,
      online: !!lobbyUsers[username] || r.members.some(m => m.username === username)
    }))
  }));
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
    const data    = await res.json();
    const balance = (data.result && data.result.length > 0)
      ? parseFloat(data.result[0].balance) || 0
      : 0;
    tokenBalanceCache[cacheKey] = { balance, fetchedAt: Date.now() };
    return balance;
  } catch(e) {
    console.warn(`[token] Balance check failed ${symbol}/@${account}: ${e.message}`);
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
    text:              0,
    textSession:       0,
    voiceRing:         0, voiceConnect:    0, voiceRate:         0,
    voiceMinDepositMin: 10, voiceMinDepositHbd: null,
    videoRing:         0, videoConnect:    0, videoRate:         0,
    videoMinDepositMin: 10, videoMinDepositHbd: null
  };

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
          return { currency: bypassToken, ...buildCallRateResult(tokSection, callType, escrow, platformFee) };
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
      return { currency: tok.symbol, ...buildCallRateResult(tok, callType, escrow, platformFee) };
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
      return { currency: 'HBD', ...buildCallRateResult(win, callType, escrow, platformFee) };
    }
  }

  // ── Step 4: Default list ───────────────────────────────────────────────────
  const defList = (calleeRates.lists || []).find(l => l.name === 'default');
  if (defList) {
    const win = defList.windows.find(w =>
      w.days.includes(dayName) && timeInWindow(timeStr, w.timeStart, w.timeEnd)
    );
    if (win) {
      return { currency: 'HBD', ...buildCallRateResult(win, callType, escrow, platformFee) };
    }
  }

  // ── Step 5: No rates apply → free call ────────────────────────────────────
  return null;
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
  for (const node of nodes) {
    try {
      const res  = await fetch(node, {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify(body),
        signal:  AbortSignal.timeout(8000)
      });
      const data = await res.json();
      if (data.result !== undefined) return data;
    } catch(e) {
      console.warn(`[hive] Node ${node} failed: ${e.message} — trying next`);
    }
  }
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
  res.json({ found: true, version: rates.version, rates, applicable, testedWith: { caller, callType, time: new Date().toISOString() } });
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

    socket.emit('lobby-users', lobbySnapshot());
    socket.emit('lobby-rooms', roomsSnapshot());
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

  socket.on('lobby-chat', ({ message, signature, timestamp }) => {
    const from = socket._username;
    if (!from) return;
    io.emit('lobby-chat', { from, message, signature, timestamp });
  });

  // ── LOBBY DM — rate-checked and payment-enforced ──────────────────────────
  // Free DMs: emitted with no payment fields → relayed immediately.
  // ── LOBBY ENCRYPTED — toggle path ─────────────────────────────────────────
  // Sent when a user has another user toggled on in the lobby user list.
  // Ephemeral: server only relays, no billing, no storage, no verification.
  // The ciphertext is encrypted with the recipient's public key — server never sees plaintext.

  socket.on('lobby-encrypted', ({ to, ciphertext, senderCiphertext, signature, timestamp }) => {
    const from = socket._username;
    if (!from) return;
    const recipient = lobbyUsers[to];
    if (recipient) {
      io.to(recipient.socketId).emit('lobby-encrypted', { from, ciphertext, signature, timestamp });
    } else if (FEDERATION_ENABLED) {
      // Toggle messages to federated users get relayed as free DMs — ephemeral,
      // no billing, no payment verification. Store sender's copy only on this
      // side; the peer stores the recipient's copy.
      const peer = peerForUser(to);
      if (peer) {
        fedSend(peer.ws, {
          type: 'dm',
          from, to, ciphertext, signature, timestamp,
          textPaid: 0,
          fromServer: SERVER_DOMAIN
        });
      } else {
        return; // recipient went offline
      }
    } else {
      return;
    }

    // Store in chat DB (both copies)
    chatStoreDm(from, to, ciphertext, senderCiphertext || null, signature, timestamp, 0);
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
      io.to(recipient.socketId).emit('lobby-dm', { from, ciphertext, signature, timestamp, textPaid: textPaid || 0 });
    } else if (federatedTo) {
      fedSend(federatedTo.ws, {
        type: 'dm',
        from, to, ciphertext, signature, timestamp,
        textPaid:    textPaid    || 0,
        textMemo:    textMemo    || null,
        textCurrency: cur,
        msgId:       msgId       || null,
        fromServer:  SERVER_DOMAIN
      });
    }
    socket.emit('lobby-dm-sent', { to, textPaid: textPaid || 0 });

    // ── Store in chat DB (sender's local copy) ────────────────────────────
    // For federated recipients, the peer server stores the recipient's copy.
    chatStoreDm(from, to, ciphertext, senderCiphertext || null, signature, timestamp, textPaid || 0);
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

    const now     = new Date();
    const dayName = ['sun','mon','tue','wed','thu','fri','sat'][now.getDay()];
    const timeStr = now.getHours().toString().padStart(2,'0') + ':' + now.getMinutes().toString().padStart(2,'0');
    const { escrow } = rates;

    // ── Platform fee enforcement ─────────────────────────────────────────────
    const calleeFee = rates.platformFee;
    const serverFee = PLATFORM_FEE;
    const platformFee = serverFee; // best price for callee

    if (typeof calleeFee === 'number' && calleeFee < serverFee) {
      const calleePct = (calleeFee * 100).toFixed(1);
      const serverPct = (serverFee * 100).toFixed(1);
      return cb({
        found: true, feeRejected: true,
        message: `@${callee}'s platform fee (${calleePct}%) is below this server's minimum (${serverPct}%). They need to set PLATFORM-FEE to at least ${serverPct}% in their rates post to receive paid contacts on this server.`
      });
    }

    // Check blocked
    if (rates.blocked && rates.blocked.users.includes(caller.toLowerCase())) {
      const bypassToken = rates.blocked.allowIfToken;
      let bypassed = false;
      if (bypassToken) {
        const bal = await getHiveEngineTokenBalance(caller, bypassToken);
        if (bal > 0) bypassed = true;
      }
      if (!bypassed) {
        return cb({ found: true, blocked: true, message: rates.blocked.message || 'You have been blocked.' });
      }
    }

    const options = [];

    // Collect all token options the caller qualifies for
    for (const tok of (rates.tokens || [])) {
      const bal = await getHiveEngineTokenBalance(caller, tok.symbol);
      if (bal > 0) {
        const built = buildCallRateResult(tok, callType, escrow, platformFee);
        options.push({ currency: tok.symbol, balance: bal, ...built });
      }
    }

    // Collect HBD default rate (from named lists or default list)
    const callerLower = caller.toLowerCase();
    let hbdOption = null;

    // Named lists first
    for (const list of (rates.lists || [])) {
      if (list.name === 'default') continue;
      if (!list.users.includes(callerLower)) continue;
      const win = list.windows.find(w =>
        w.days.includes(dayName) && timeInWindow(timeStr, w.timeStart, w.timeEnd)
      );
      if (win) {
        hbdOption = { currency: 'HBD', listName: list.name, ...buildCallRateResult(win, callType, escrow, platformFee) };
        break;
      }
    }

    // Default list fallback
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

    if (hbdOption) options.push(hbdOption);

    if (options.length === 0) {
      return cb({ found: true, free: true });
    }

    // Mark the "best" (auto-selected) option — first token match, or HBD
    if (options.length > 0) options[0]._recommended = true;

    cb({ found: true, free: false, options, escrow: escrow || ESCROW_ACCOUNT });
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

  // ── CALL END (explicit hang-up) ────────────────────────────────────────────

  socket.on('call-end', async ({ callId }) => {
    const username = socket._username;
    const room     = socket._room;
    if (room && rooms[room]) {
      socket.to(room).emit('peer-hung-up', { by: username });
      const cid = rooms[room].callId || callId;
      if (cid) await processCallEnd(cid, room, io, lobbyUsers);
    }
    console.log(`📵 @${username} ended call`);
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
      if (ringFeePaid && ringFeePaid > 0 && callId) {
        // Callee went offline between rate check and ring — refund ring fee
        const refundMemo = `v4call:refund:${callId}:offline`;
        ledgerPayment(callId, 'refund', ESCROW_ACCOUNT, caller, ringFeePaid, refundMemo, 'pending');
        sendFromEscrow(caller, ringFeePaid, refundMemo, 'HBD', callId).then(r => {
          const msg = r.success
            ? `@${callee} is offline. Ring fee of ${ringFeePaid.toFixed(3)} HBD refunded.`
            : `@${callee} is offline. Refund of ${ringFeePaid.toFixed(3)} HBD pending — contact support.`;
          socket.emit('call-failed', { reason: msg, refunded: r.success });
          if (r.success) ledgerPaymentUpdate(callId, 'refund', 'sent', r.txId);
        });
      } else {
        socket.emit('call-failed', { reason: `@${callee} is not online.` });
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
      creator:   caller,
      allowlist: new Set([caller, callee]),
      members:   [],
      createdAt: new Date(),
      isCall:    true,
      callType:  callType || 'voice',
      callId:    effectiveCallId,
      federated: federatedCallee ? { calleeServer: federatedCallee.domain } : null
    };

    ledgerCallCreate(effectiveCallId, caller, callee, callType || 'voice');
    ledgerCallUpdate(effectiveCallId, { status: 'ringing' });
    if (ringFeePaid > 0) {
      ledgerPayment(effectiveCallId, 'ring', caller, ESCROW_ACCOUNT, ringFeePaid,
        `v4call:ring:${effectiveCallId}:${callee}`, 'verified');
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

  socket.on('room-create', ({ roomName, invitees }) => {
    const creator = socket._username;
    if (!creator) return;
    if (rooms[roomName]) {
      socket.emit('room-create-error', `Room "${roomName}" already exists.`);
      return;
    }
    const allowlist = new Set([creator, ...invitees]);
    rooms[roomName] = { creator, allowlist, members: [], createdAt: new Date() };
    for (const invitee of invitees) {
      if (invitee === creator) continue;
      const lu = lobbyUsers[invitee];
      if (lu) io.to(lu.socketId).emit('room-invite', { roomName, from: creator, invitees });
    }
    broadcastRooms();
    socket.emit('room-created', { roomName, invitees: [...invitees] });
    console.log(`@${creator} created #${roomName}`);
  });

  socket.on('request-join-token', ({ roomName }, cb) => {
    const username = socket._username;
    const pubKey   = socket._pubKey;
    if (!username || !pubKey) return cb({ error: 'Not authenticated' });
    cb({ token: generateToken(username, pubKey, roomName) });
  });

  // ── ROOM JOINING ───────────────────────────────────────────────────────────

  socket.on('join', ({ room, username, pubKey }) => {
    if (rooms[room] && !rooms[room].allowlist.has(username)) {
      socket.emit('join-rejected', { room, reason: `You are not on the allowlist for room "${room}".` });
      return;
    }
    if (!rooms[room]) {
      rooms[room] = { creator: username, allowlist: new Set([username]), members: [], createdAt: new Date() };
    }
    socket.join(room);
    socket._room = room;
    rooms[room].members.push({ socketId: socket.id, username, pubKey });
    if (lobbyUsers[username]) lobbyUsers[username].inRoom = room;

    const everyone = rooms[room].members.map(u => ({ socketId: u.socketId, username: u.username, pubKey: u.pubKey }));
    socket.emit('room-users', everyone);
    socket.emit('room-info', { creator: rooms[room].creator, allowlist: [...rooms[room].allowlist] });

    // Send room history (broadcasts + messages encrypted to this user)
    const history = chatGetRoomHistory(room, username);
    if (history.length > 0) {
      socket.emit('room-history', history);
    }

    socket.to(room).emit('user-joined', { socketId: socket.id, username, pubKey });
    broadcastRooms();
    console.log(`@${username} joined #${room} (${rooms[room].members.length} members)`);
  });

  // ── ALLOWLIST MANAGEMENT ───────────────────────────────────────────────────

  socket.on('allowlist-add', ({ room, username: targetUser }) => {
    const r = rooms[room];
    if (!r || r.creator !== socket._username) return;
    r.allowlist.add(targetUser);
    const lu = lobbyUsers[targetUser];
    if (lu) io.to(lu.socketId).emit('room-invite', { roomName: room, from: socket._username, invitees: [...r.allowlist] });
    io.to(room).emit('room-info', { creator: r.creator, allowlist: [...r.allowlist] });
    broadcastRooms();
  });

  socket.on('allowlist-remove', ({ room, username: targetUser }) => {
    const r = rooms[room];
    if (!r || r.creator !== socket._username) return;
    if (targetUser === r.creator) return;
    r.allowlist.delete(targetUser);
    const member = r.members.find(m => m.username === targetUser);
    if (member) io.to(member.socketId).emit('kicked', { room, reason: 'You were removed from this room.' });
    io.to(room).emit('room-info', { creator: r.creator, allowlist: [...r.allowlist] });
    broadcastRooms();
  });

  // ── RESYNC ─────────────────────────────────────────────────────────────────

  socket.on('resync', () => {
    const room = socket._room;
    if (!room || !rooms[room]) return;
    const everyone = rooms[room].members.map(u => ({ socketId: u.socketId, username: u.username, pubKey: u.pubKey }));
    socket.emit('room-users-resync', everyone);
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

      if (rooms[room].isCall) {
        socket.to(room).emit('peer-hung-up', { by: username });
        const callId = rooms[room].callId;
        if (callId && activePayments[callId]) {
          processCallEnd(callId, room, io, lobbyUsers);
        }
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

    if (username && lobbyUsers[username]) {
      delete lobbyUsers[username];
      broadcastLobby();
      broadcastRooms();
      if (FEDERATION_ENABLED) fedAnnounceUserOffline(username);
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
const PEER_VERIFY_TTL_OK   = 60 * 60 * 1000;   // 1h for positive results
const PEER_VERIFY_TTL_FAIL = 5  * 60 * 1000;   // 5min for failures
const peerVerifyCache = {};                    // domain → { result, cachedAt }

async function _fetchPeerVerifyFile(domain) {
  const url = `https://${domain}/.well-known/v4call-server.json`;
  const res = await fetch(url, { signal: AbortSignal.timeout(8000) });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
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

function _verifyPayloadString(obj) {
  return [
    obj.claim, obj.domain, obj.hive_account,
    obj.escrow, obj.fee_account, obj.federation_ws,
    obj.issued, obj.expires || '', obj.nonce
  ].join('|');
}

async function verifyPeer(domain, claimedAccount) {
  const cached = peerVerifyCache[domain];
  if (cached) {
    const ttl = cached.result.verified ? PEER_VERIFY_TTL_OK : PEER_VERIFY_TTL_FAIL;
    if ((Date.now() - cached.cachedAt) < ttl) return cached.result;
  }

  const fail = (reason) => {
    const r = { verified: false, reason };
    peerVerifyCache[domain] = { result: r, cachedAt: Date.now() };
    return r;
  };

  let obj;
  try { obj = await _fetchPeerVerifyFile(domain); }
  catch(e) { return fail(`Cannot fetch verify.json: ${e.message}`); }

  // Shape + field-consistency checks
  if (obj.claim !== 'v4call-server-ownership')                 return fail('wrong claim');
  if (!obj.domain || !obj.hive_account || !obj.signature)      return fail('missing required fields');
  if (obj.domain.toLowerCase() !== domain.toLowerCase())       return fail(`domain mismatch (file claims ${obj.domain})`);
  if (claimedAccount && obj.hive_account.toLowerCase() !== claimedAccount.toLowerCase()) {
    return fail(`account mismatch: hello says @${claimedAccount} but file signed by @${obj.hive_account}`);
  }

  // Expiry — only enforce if operator set one
  if (obj.expires) {
    const exp = Date.parse(obj.expires);
    if (Number.isFinite(exp) && Date.now() > exp) return fail(`expired on ${obj.expires}`);
  }
  // Reject far-future issued (tolerates 5min clock skew)
  if (obj.issued) {
    const iss = Date.parse(obj.issued);
    if (Number.isFinite(iss) && iss > Date.now() + 5 * 60 * 1000) return fail(`issued timestamp in future: ${obj.issued}`);
  }

  // Signature verification
  let pubKeyStr;
  try { pubKeyStr = await _lookupPostingPubKey(obj.hive_account); }
  catch(e) { return fail(`Hive lookup error: ${e.message}`); }
  if (!pubKeyStr) return fail(`no posting key found for @${obj.hive_account}`);

  try {
    const payload = _verifyPayloadString(obj);
    const hash    = dhive.cryptoUtils.sha256(payload);
    const pubKey  = dhive.PublicKey.fromString(pubKeyStr);
    const sig     = dhive.Signature.fromString(obj.signature);
    if (!pubKey.verify(hash, sig)) return fail('signature does not match posting key');
  } catch(e) {
    return fail(`signature parse error: ${e.message}`);
  }

  const ok = {
    verified:      true,
    domain:        obj.domain,
    hive_account:  obj.hive_account,
    escrow:        obj.escrow,
    fee_account:   obj.fee_account,
    federation_ws: obj.federation_ws,
    issued:        obj.issued,
    expires:       obj.expires || null
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
  const data = await hivePost({
    jsonrpc: '2.0',
    method:  'condenser_api.get_discussions_by_created',
    params:  [{ tag: 'v4call-server', limit: 50 }],
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
      last_seen:     now
    };
    if (vr.verified) verifiedCount++;
  }
  console.log(`[discovery] Scan complete — ${parsedCount} v4call-server post(s), ${verifiedCount} verified`);
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
      peer.verified      = true;
      peer.hive_account  = vr.hive_account;
      peer.fee_account   = vr.fee_account;
      peer.issued        = vr.issued;
      peer.expires       = vr.expires;

      if (msg.escrow && msg.escrow !== vr.escrow) {
        console.warn(`[federation] ⚠ ${domain}: hello.escrow=@${msg.escrow} but verify.json.escrow=@${vr.escrow} — using signed value`);
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
            fromServer: fromServer || ws._domain
          });
        }
        chatStoreDm(from, to, ciphertext, null, signature, timestamp, paid);
        fedSend(ws, { type: 'dm-delivered', from, to, msgId: msgId || null });
      };

      if (paid >= 0.001 && textMemo && msgId) {
        // Re-verify on-chain (caller's server already verified, but trust-but-verify)
        const verifier = (cur !== 'HBD' && cur !== 'HIVE')
          ? verifyHiveEnginePayment(from, ESCROW_ACCOUNT, paid, cur, textMemo)
          : verifyHivePayment(from, ESCROW_ACCOUNT, paid, textMemo);
        verifier.then(ok => {
          if (!ok) {
            console.warn(`[fed-text] ✗ Payment re-verify failed: @${from} → @${ESCROW_ACCOUNT} ${paid} ${cur} (memo: ${textMemo})`);
            fedSend(ws, { type: 'dm-failed', from, to, msgId, reason: 'payment not on chain' });
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
      verifier.then(ok => {
        if (!ok) {
          console.warn(`[fed-payment] ✗ Re-verify failed for ${paymentType} ${callId} (@${from} → @${ESCROW_ACCOUNT} ${amount} ${cur})`);
          fedSend(ws, { type: 'payment-rejected', callId, paymentType, reason: 'on-chain not found' });
          return;
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
          if (msg.ratePerHour !== undefined) p.ratePerHour = msg.ratePerHour;
          if (msg.platformFee !== undefined) p.posterPlatformFee = msg.platformFee;
          if (msg.callType    !== undefined) p.callType    = msg.callType;
          ledgerCallCreate(callId, from, to, msg.callType || 'voice');
          ledgerPayment(callId, 'ring', from, ESCROW_ACCOUNT, amount, memo, 'verified');
        } else if (paymentType === 'deposit') {
          p.depositPaid     = msg.depositAmount || amount;
          p.connectPaid     = msg.connectAmount || 0;
          p.creditRemaining = p.depositPaid;
          ledgerPayment(callId, 'deposit', from, ESCROW_ACCOUNT, amount, memo, 'verified');
        } else if (paymentType === 'topup') {
          p.depositPaid     = (p.depositPaid     || 0) + amount;
          p.creditRemaining = (p.creditRemaining || 0) + amount;
          ledgerPayment(callId, 'topup', from, ESCROW_ACCOUNT, amount, memo, 'verified');
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
      federationPeers[ws._domain].connected = false;
      console.log(`[federation] ${label} to ${ws._domain} closed`);
      broadcastLobby();
    }
  });
  ws.on('error', (e) => {
    console.warn(`[federation] ${label} error (${ws._domain || 'unidentified'}): ${e.message}`);
  });
  // Say hello immediately. Include our ESCROW_ACCOUNT so peers can detect
  // rates-post escrow mismatches, and our SERVER_HIVE_ACCOUNT so peers can
  // pin their verify.json check to the correct signer.
  fedSend(ws, {
    type:         'hello',
    domain:       SERVER_DOMAIN,
    name:         SERVER_NAME,
    version:      FEDERATION_VERSION,
    escrow:       ESCROW_ACCOUNT,
    hive_account: SERVER_HIVE_ACCOUNT
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

if (FEDERATION_ENABLED) {
  loadApprovedPeers();
  console.log(`[federation] Approved peers: ${[...approvedPeers].join(', ') || '(none)'}`);
  for (const url of FEDERATION_PEERS) fedConnectPeer(url);
  // Kick off discovery shortly after startup (non-blocking), then every 2 hours.
  setTimeout(() => {
    scanV4CallDirectory().catch(e => console.error('[discovery] scan error:', e.message));
  }, 5000);
  setInterval(() => {
    scanV4CallDirectory().catch(e => console.error('[discovery] scan error:', e.message));
  }, 2 * 60 * 60 * 1000);
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
