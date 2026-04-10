// Load .env file if present (for local dev and Docker)
// In production via systemd, env vars are set directly in the service file
try { require('dotenv').config(); } catch(e) { /* dotenv optional */ }

const express     = require('express');
const http        = require('http');
const { Server }  = require('socket.io');
const path        = require('path');
const crypto      = require('crypto');
const fs          = require('fs');
const Database    = require('better-sqlite3');
const dhive       = require('@hiveio/dhive');

const app    = express();
const server = http.createServer(app);
const io     = new Server(server);

app.use(express.static(path.join(__dirname, 'public')));

// ── Config ────────────────────────────────────────────────────────────────────
// ── Config — all values read from environment variables (.env / systemd) ─────
// Edit .env file to configure your server — do not hardcode values here
const SERVER_NAME         = process.env.SERVER_NAME         || 'v4call';
const SERVER_DOMAIN       = process.env.SERVER_DOMAIN       || 'v4call.com';
const SERVER_HIVE_ACCOUNT = process.env.SERVER_HIVE_ACCOUNT || 'v4call';
const ESCROW_ACCOUNT      = process.env.ESCROW_ACCOUNT      || 'v4call-escrow';
const PLATFORM_FEE        = parseFloat(process.env.DEFAULT_PLATFORM_FEE || '10') / 100;
const PORT                = parseInt(process.env.PORT        || '3000');
const BIND_HOST           = process.env.BIND_HOST            || '127.0.0.1';

// Hive API nodes — can override first node via HIVE_API env var
const HIVE_API        = process.env.HIVE_API || 'https://api.hive.blog';
const HIVE_API_NODES  = [
  HIVE_API,
  'https://anyx.io',
  'https://api.deathwing.me',
  'https://hived.emre.sh'
].filter((v, i, a) => a.indexOf(v) === i); // deduplicate

// Call behaviour — tunable via env vars
const CALL_COOLDOWN_MS      = parseInt(process.env.CALL_COOLDOWN_MS      || '30000');
const MAX_CALL_DURATION_MIN = parseInt(process.env.MAX_CALL_DURATION_MIN  || '120');
const PAYMENT_VERIFY_RETRIES    = parseInt(process.env.PAYMENT_VERIFY_RETRIES    || '3');
const PAYMENT_VERIFY_DELAY_MS   = parseInt(process.env.PAYMENT_VERIFY_DELAY_MS   || '5000');

console.log(`[config] Server: ${SERVER_NAME} (${SERVER_DOMAIN})`);
console.log(`[config] Escrow: @${ESCROW_ACCOUNT}`);
console.log(`[config] Platform fee: ${PLATFORM_FEE * 100}%`);
console.log(`[config] Max call duration: ${MAX_CALL_DURATION_MIN} min`);

// ── SQLite Ledger ─────────────────────────────────────────────────────────────
const LOG_DIR = path.join(__dirname, 'logs');
if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true });

const db = new Database(path.join(LOG_DIR, 'v4call-ledger.db'));

// Create tables
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
    status          TEXT    NOT NULL DEFAULT 'initiated',  -- 'initiated'|'ringing'|'connected'|'ended'|'missed'|'declined'
    end_reason      TEXT    -- 'caller_hung_up'|'callee_hung_up'|'timeout'|'declined'|'cap_reached'
  );

  CREATE INDEX IF NOT EXISTS idx_calls_caller  ON calls(caller);
  CREATE INDEX IF NOT EXISTS idx_calls_callee  ON calls(callee);
  CREATE INDEX IF NOT EXISTS idx_calls_call_id ON calls(call_id);
  CREATE INDEX IF NOT EXISTS idx_payments_call_id ON payments(call_id);
`);

// Ledger helper functions
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
  const set = keys.map(k => k + ' = ?').join(', ');
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

console.log('[ledger] SQLite database ready:', path.join(LOG_DIR, 'v4call-ledger.db'));

// ── Lobby ─────────────────────────────────────────────────────────────────────
const lobbyUsers = {};

function lobbySnapshot() {
  return Object.entries(lobbyUsers)
    .filter(([, u]) => !u.invisible)
    .map(([username, u]) => ({ username, socketId: u.socketId, pubKey: u.pubKey }));
}
function broadcastLobby() { io.emit('lobby-users', lobbySnapshot()); }

// ── Rooms ─────────────────────────────────────────────────────────────────────
const rooms = {};

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

// ── Rate cache (TTL: 10 minutes) ──────────────────────────────────────────────
const rateCache = {};
const RATE_CACHE_TTL = 10 * 60 * 1000;

async function fetchRates(username) {
  const cached = rateCache[username];
  if (cached && (Date.now() - cached.fetchedAt) < RATE_CACHE_TTL) {
    return cached.rates;
  }
  try {
    // Try direct permlink first (most reliable — permlink is always 'v4call-rates')
    const directRes = await fetch(HIVE_API, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0', method: 'condenser_api.get_content',
        params: [username, 'v4call-rates'], id: 1
      })
    });
    const directData = await directRes.json();
    if (directData.result && directData.result.author === username && directData.result.body) {
      const rates = parseRates(directData.result.body);
      if (rates) {
        rateCache[username] = { rates, fetchedAt: Date.now() };
        console.log(`[rates] Fetched rates for @${username} via direct permlink`);
        return rates;
      }
    }

    // Fallback: search recent blog posts for title 'v4call-rates'
    // Handles cases where permlink was customised (e.g. 'v4call-rates-1234')
    const blogRes  = await fetch(HIVE_API, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0', method: 'condenser_api.get_discussions_by_author_before_date',
        params: [username, '', '2099-01-01T00:00:00', 20], id: 1
      })
    });
    const blogData = await blogRes.json();
    if (!blogData.result) return null;

    // Find the most recent post whose title matches (case-insensitive)
    const post = blogData.result.find(p =>
      p.title.toLowerCase() === 'v4call-rates' && p.author === username
    );
    if (!post) {
      console.log(`[rates] No v4call-rates post found for @${username}`);
      return null;
    }

    const rates = parseRates(post.body);
    if (rates) {
      rateCache[username] = { rates, fetchedAt: Date.now() };
      console.log(`[rates] Fetched rates for @${username} via blog search`);
    }
    return rates;
  } catch(e) {
    console.error(`[rates] Failed to fetch rates for @${username}:`, e.message);
    return null;
  }
}

// ── Rate Parser ───────────────────────────────────────────────────────────────
// Parses [V4CALL-RATES-V1] format from a Hive post body
function parseRates(body) {
  const result = {
    account:     '',
    platformFee: 0.10,
    escrow:      ESCROW_ACCOUNT,
    lists:       []
  };

  // Check for the rates block
  const blockMatch = body.match(/\[V4CALL-RATES-V1\]([\s\S]*?)\[\/V4CALL-RATES-V1\]/i);
  if (!blockMatch) return null;
  const block = blockMatch[1];

  // Parse top-level fields
  const accountMatch = block.match(/^ACCOUNT:(.+)$/m);
  if (accountMatch) result.account = accountMatch[1].trim();

  const feeMatch = block.match(/^PLATFORM-FEE:(\d+(?:\.\d+)?)%/m);
  if (feeMatch) result.platformFee = parseFloat(feeMatch[1]) / 100;

  const escrowMatch = block.match(/^ESCROW:(.+)$/m);
  if (escrowMatch) result.escrow = escrowMatch[1].trim();

  // Parse each LIST block
  const listRegex = /\[LIST:([^\]]+)\]([\s\S]*?)\[\/LIST\]/gi;
  let listMatch;
  while ((listMatch = listRegex.exec(block)) !== null) {
    const listName = listMatch[1].trim();
    const listBody = listMatch[2];

    const list = { name: listName, users: [], windows: [] };

    // Parse USERS line
    const usersMatch = listBody.match(/^USERS:(.+)$/m);
    if (usersMatch) {
      list.users = usersMatch[1].split(',').map(u => u.trim().toLowerCase()).filter(Boolean);
    }

    // Parse TIME windows within this list
    const timeRegex = /\[DAYS:([^\]]+)\]\[TIME:([^\]]+)\]([\s\S]*?)\[\/TIME\]/gi;
    let timeMatch;
    while ((timeMatch = timeRegex.exec(listBody)) !== null) {
      const daysStr  = timeMatch[1].trim();
      const timeStr  = timeMatch[2].trim();
      const timeBody = timeMatch[3];

      // Parse days
      const days = parseDays(daysStr);

      // Parse time range
      const timeParts = timeStr.split('-');
      const timeStart = timeParts[0]?.trim() || '00:00';
      const timeEnd   = timeParts[1]?.trim() || '23:59';

      // Parse rates
      const window = { days, timeStart, timeEnd };

      const textMatch = timeBody.match(/^TEXT:(.+)$/m);
      if (textMatch) window.text = parseHbd(textMatch[1]);

      const voiceMatch = timeBody.match(/^VOICE:(.+)$/m);
      if (voiceMatch) {
        const v = parseRateLine(voiceMatch[1]);
        window.voiceRing         = v.ring;
        window.voiceConnect      = v.connect;
        window.voiceRate         = v.rate;
        window.voiceMinDepMin    = v.minDepositMin;
        window.voiceMinDepHbd    = v.minDepositHbd;
      }

      const videoMatch = timeBody.match(/^VIDEO:(.+)$/m);
      if (videoMatch) {
        const v = parseRateLine(videoMatch[1]);
        window.videoRing         = v.ring;
        window.videoConnect      = v.connect;
        window.videoRate         = v.rate;
        window.videoMinDepMin    = v.minDepositMin;
        window.videoMinDepHbd    = v.minDepositHbd;
      }

      list.windows.push(window);
    }

    result.lists.push(list);
  }

  return result;
}

function parseDays(daysStr) {
  const ALL_DAYS = ['mon','tue','wed','thu','fri','sat','sun'];
  if (daysStr === 'mon-sun') return ALL_DAYS;
  if (daysStr === 'mon-fri') return ['mon','tue','wed','thu','fri'];
  if (daysStr === 'sat-sun') return ['sat','sun'];
  return daysStr.split(',').map(d => d.trim().toLowerCase());
}

function parseHbd(str) {
  const m = str.match(/([\d.]+)\s*HBD/i);
  return m ? parseFloat(m[1]) : 0;
}

function parseRateLine(str) {
  const ring       = str.match(/RING:([\d.]+)\s*HBD/i);
  const connect    = str.match(/CONNECT:([\d.]+)\s*HBD/i);
  const rate       = str.match(/RATE:([\d.]+)\s*HBD/i);
  // MIN-DEPOSIT can be specified as minutes: MIN-DEPOSIT:10min
  // or as a fixed HBD amount: MIN-DEPOSIT:0.500 HBD
  const minDepMin  = str.match(/MIN-DEPOSIT:([\d.]+)\s*min/i);
  const minDepHbd  = str.match(/MIN-DEPOSIT:([\d.]+)\s*HBD/i);
  return {
    ring:         ring      ? parseFloat(ring[1])      : 0,
    connect:      connect   ? parseFloat(connect[1])   : 0,
    rate:         rate      ? parseFloat(rate[1])      : 0,
    minDepositMin: minDepMin ? parseFloat(minDepMin[1]) : null,  // minutes
    minDepositHbd: minDepHbd ? parseFloat(minDepHbd[1]) : null   // fixed HBD
  };
}

// ── Calculate minimum deposit amount ─────────────────────────────────────────
// Returns the HBD amount caller must deposit upfront for call credit
function calcMinDeposit(ratePerHour, minDepositMin, minDepositHbd) {
  // Fixed HBD amount takes priority
  if (minDepositHbd && minDepositHbd > 0) return minDepositHbd;
  // Minutes-based deposit
  const minutes = minDepositMin || 10; // default 10 minutes
  return parseFloat(((ratePerHour / 60) * minutes).toFixed(3));
}

// ── Rate Lookup ───────────────────────────────────────────────────────────────
// Given a callee's rates, caller's username, call type, and current time,
// return the applicable rates object
function getRatesForCaller(rates, callerUsername, callType, now) {
  if (!rates || !rates.lists.length) return null;

  const dayName  = ['sun','mon','tue','wed','thu','fri','sat'][now.getDay()];
  const timeStr  = now.getHours().toString().padStart(2,'0') + ':' + now.getMinutes().toString().padStart(2,'0');

  // Find which list the caller belongs to (first match wins)
  // Default list has no users array — it's the fallback
  let matchedList = null;
  for (const list of rates.lists) {
    if (list.name === 'default') continue; // check default last
    if (list.users.includes(callerUsername)) { matchedList = list; break; }
  }
  if (!matchedList) {
    matchedList = rates.lists.find(l => l.name === 'default');
  }
  if (!matchedList) return null;

  // Find the matching time window
  for (const window of matchedList.windows) {
    if (!window.days.includes(dayName)) continue;
    if (!timeInWindow(timeStr, window.timeStart, window.timeEnd)) continue;

    if (callType === 'text') {
      return { type: 'text', flat: window.text || 0, escrow: rates.escrow, platformFee: rates.platformFee };
    }
    if (callType === 'voice') {
      const r = window.voiceRate || 0;
      const deposit = calcMinDeposit(r, window.voiceMinDepMin, window.voiceMinDepHbd);
      return { type: 'voice',
               ring: window.voiceRing || 0,
               connect: window.voiceConnect || 0,
               rate: r,
               minDeposit: deposit,
               minDepositMin: window.voiceMinDepMin || 10,
               escrow: rates.escrow,
               platformFee: rates.platformFee };
    }
    if (callType === 'video') {
      const r = window.videoRate || 0;
      const deposit = calcMinDeposit(r, window.videoMinDepMin, window.videoMinDepHbd);
      return { type: 'video',
               ring: window.videoRing || 0,
               connect: window.videoConnect || 0,
               rate: r,
               minDeposit: deposit,
               minDepositMin: window.videoMinDepMin || 10,
               escrow: rates.escrow,
               platformFee: rates.platformFee };
    }
  }

  return null;
}

function timeInWindow(time, start, end) {
  // Handle windows that cross midnight (e.g. 23:00-07:00)
  if (start <= end) {
    return time >= start && time <= end;
  } else {
    return time >= start || time <= end;
  }
}

// ── Payment tracking ──────────────────────────────────────────────────────────
// activePayments: callId → { caller, callee, ringPaid, depositPaid, creditRemaining,
//                            startTime, ratePerHour, escrow, platformFee, _processing }
const activePayments = {};

// ── Credit burn engine ────────────────────────────────────────────────────────
// Tracks remaining credit per call and disconnects when exhausted
const creditTimers = {}; // callId → intervalId

function startCreditBurn(callId, roomName) {
  const payment = activePayments[callId];
  if (!payment || !payment.ratePerHour) return; // free call — no burn needed

  const ratePerMs  = payment.ratePerHour / (60 * 60 * 1000);
  let   warned5    = false;
  let   warned2    = false;

  creditTimers[callId] = setInterval(async () => {
    const p = activePayments[callId];
    if (!p) { clearInterval(creditTimers[callId]); return; }

    const elapsed  = Date.now() - (p.startTime || Date.now());
    const burned   = elapsed * ratePerMs;
    p.creditRemaining = Math.max(0, (p.depositPaid || 0) - burned);

    const minLeft  = p.creditRemaining / (p.ratePerHour / 60);

    // 5-minute warning
    if (!warned5 && minLeft <= 5 && minLeft > 2) {
      warned5 = true;
      io.to(roomName).emit('credit-warning', {
        minutesLeft: parseFloat(minLeft.toFixed(1)),
        creditLeft:  parseFloat(p.creditRemaining.toFixed(3)),
        level: '5min'
      });
    }

    // 2-minute warning
    if (!warned2 && minLeft <= 2 && minLeft > 0) {
      warned2 = true;
      io.to(roomName).emit('credit-warning', {
        minutesLeft: parseFloat(minLeft.toFixed(1)),
        creditLeft:  parseFloat(p.creditRemaining.toFixed(3)),
        level: '2min'
      });
    }

    // Credit exhausted — disconnect
    if (p.creditRemaining <= 0) {
      clearInterval(creditTimers[callId]);
      delete creditTimers[callId];
      console.log(`[credit] Call ${callId} ran out of credit — disconnecting`);
      io.to(roomName).emit('credit-exhausted', { callId });
      await processCallEnd(callId, roomName, io, lobbyUsers, 'credit_exhausted');
    }
  }, 10000); // check every 10 seconds — fine enough for warnings
}

function stopCreditBurn(callId) {
  if (creditTimers[callId]) {
    clearInterval(creditTimers[callId]);
    delete creditTimers[callId];
  }
}

// callCooldowns: "caller->callee" → timestamp of last attempt
// Prevents spam-ringing the same user
const callCooldowns = {};

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

// Clean up cooldowns older than 5 minutes
setInterval(() => {
  const cutoff = Date.now() - 5 * 60 * 1000;
  for (const k in callCooldowns) {
    if (callCooldowns[k] < cutoff) delete callCooldowns[k];
  }
}, 60000);

// ── Hive API helper — tries multiple nodes ────────────────────────────────────
async function hivePost(body, nodes = HIVE_API_NODES) {
  for (const node of nodes) {
    try {
      const res = await fetch(node, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(8000)
      });
      const data = await res.json();
      if (data.result !== undefined) return data;
    } catch(e) {
      console.warn(`[hive] Node ${node} failed: ${e.message} — trying next`);
    }
  }
  return null;
}

// ── Check escrow HBD balance ───────────────────────────────────────────────────
async function getEscrowBalance() {
  const data = await hivePost({
    jsonrpc: '2.0', method: 'condenser_api.get_accounts',
    params: [[ESCROW_ACCOUNT]], id: 1
  });
  if (!data?.result?.[0]) return 0;
  const hbd = data.result[0].hbd_balance || data.result[0].sbd_balance || '0 HBD';
  return parseFloat(hbd.split(' ')[0]);
}

// ── Send from escrow with balance check ───────────────────────────────────────
async function sendFromEscrow(to, amount, memo, currency = 'HBD', callId = null) {
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

  // Check escrow has sufficient balance
  const balance = await getEscrowBalance();
  if (balance < amount) {
    console.error(`[escrow] Insufficient balance: have ${balance} HBD, need ${amount} HBD for @${to}`);
    if (callId) ledgerPayment(callId, 'payout', ESCROW_ACCOUNT, to, amount, memo, 'failed', null);
    return { success: false, reason: `Escrow balance insufficient (${balance.toFixed(3)} HBD available)` };
  }

  try {
    // Use top-level required dhive (not dynamic import — avoids ESM/CJS mismatch)
    const client    = new dhive.Client(HIVE_API_NODES);
    const key       = dhive.PrivateKey.fromString(escrowKey);
    const amountStr = amount.toFixed(3) + ' ' + currency;

    console.log(`[escrow] Attempting transfer: ${amountStr} from @${ESCROW_ACCOUNT} to @${to}`);
    console.log(`[escrow] Memo: ${memo}`);

    const result = await client.broadcast.transfer({
      from: ESCROW_ACCOUNT, to, amount: amountStr, memo
    }, key);

    console.log(`[escrow] ✓ SUCCESS — Sent ${amountStr} to @${to} — tx: ${result.id}`);
    if (callId) ledgerPayment(callId, 'payout', ESCROW_ACCOUNT, to, amount, memo, 'sent', result.id);
    return { success: true, txId: result.id, amount: amountStr };
  } catch(e) {
    console.error(`[escrow] ✗ TRANSFER FAILED to @${to}:`);
    console.error(`[escrow]   Amount: ${amount} ${currency}`);
    console.error(`[escrow]   Error:  ${e.message}`);
    console.error(`[escrow]   Stack:  ${e.stack}`);
    if (callId) ledgerPayment(callId, 'payout', ESCROW_ACCOUNT, to, amount, memo, 'failed');
    return { success: false, reason: e.message };
  }
}

// ── Process call end — calculate bill, disburse, send receipts ─────────────────
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
  stopCreditBurn(callId); // Stop the credit burn timer

  const now         = Date.now();
  const startTime   = payment.startTime || now;
  const durationMs  = payment.startTime ? (now - startTime) : 0;
  // Cap duration at MAX_CALL_DURATION_MIN
  const durationMin = Math.min(durationMs / 60000, MAX_CALL_DURATION_MIN);
  const durationHr  = durationMin / 60;

  const ratePerHour  = payment.ratePerHour  || 0;
  const depositPaid  = payment.depositPaid  || 0;  // credit deposit (refundable portion)
  const connectPaid  = payment.connectPaid  || 0;  // connect fee paid upfront in combined payment
  const ringPaid     = payment.ringPaid     || 0;  // ring fee (non-refundable, goes to platform)
  const platformFee  = payment.platformFee  || 0.10;

  // ── Money flow ──────────────────────────────────────────────────────────────
  // Total received by escrow = ringPaid + connectPaid + depositPaid
  // ring fee    → platform (non-refundable service charge)
  // connect fee → callee (non-refundable answer fee, minus platform %)
  // deposit     → duration cost to callee, remainder refunded to caller

  // Duration charge (prorated, capped at deposit)
  const durationCost   = parseFloat(Math.min(ratePerHour * durationHr, depositPaid).toFixed(3));

  // Unused deposit → refund to caller
  const refundAmount   = parseFloat(Math.max(0, depositPaid - durationCost).toFixed(3));

  // Callee earns: connect fee + duration cost, minus platform %
  const calleeGross    = parseFloat((connectPaid + durationCost).toFixed(3));
  const platformOnCall = parseFloat((calleeGross * platformFee).toFixed(3));
  const calleeNet      = parseFloat((calleeGross - platformOnCall).toFixed(3));

  // Platform earns: ring fee (flat) + % of callee earnings
  const platformTotal  = parseFloat((ringPaid + platformOnCall).toFixed(3));

  // Verify escrow accounting (should sum to 0)
  const totalIn  = ringPaid + connectPaid + depositPaid;
  const totalOut = calleeNet + refundAmount + platformTotal;
  const delta    = parseFloat((totalIn - totalOut).toFixed(3));
  if (Math.abs(delta) > 0.002) {
    console.warn(`[billing] ⚠ Accounting delta: in=${totalIn} out=${totalOut} diff=${delta} HBD`);
  }

  console.log(`[billing] Call ${callId} ended (${endReason})`);
  console.log(`[billing]   Duration:    ${durationMin.toFixed(2)} min`);
  console.log(`[billing]   Ring paid:   ${ringPaid} HBD → platform`);
  console.log(`[billing]   Connect:     ${connectPaid} HBD → callee`);
  console.log(`[billing]   Duration $:  ${durationCost} HBD → callee`);
  console.log(`[billing]   Refund:      ${refundAmount} HBD → caller`);
  console.log(`[billing]   Platform:    ${platformTotal} HBD (ring + ${platformOnCall.toFixed(3)} cut)`);
  console.log(`[billing]   Callee net:  ${calleeNet} HBD`);

  const receipt = {
    callId,
    caller:         payment.caller,
    callee:         payment.callee,
    startTime:      new Date(startTime).toISOString(),
    endTime:        new Date(now).toISOString(),
    durationMin:    parseFloat(durationMin.toFixed(2)),
    ringPaid,
    connectPaid,
    depositPaid,
    durationCost,
    refundAmount,
    calleeNet,
    platformTotal,
    platformOnCall,
    currency:       'HBD',
    endReason
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

  // Send receipts
  const callerSid = lobbyUsers[payment.caller]?.socketId;
  const calleeSid = lobbyUsers[payment.callee]?.socketId;
  if (callerSid) io.to(callerSid).emit('call-receipt', { ...receipt, perspective: 'caller' });
  if (calleeSid) io.to(calleeSid).emit('call-receipt', { ...receipt, perspective: 'callee' });

  // ── Disburse from escrow ────────────────────────────────────────────────────

  // 1. Callee payout (connect fee + duration cost, minus platform %)
  if (calleeNet >= 0.001) {
    const payoutMemo = `v4call:payout:${callId}:${durationMin.toFixed(1)}min`;
    ledgerPayment(callId, 'payout', ESCROW_ACCOUNT, payment.callee, calleeNet, payoutMemo, 'pending');
    const result = await sendFromEscrow(payment.callee, calleeNet, payoutMemo, 'HBD', callId);
    if (!result.success) {
      console.error(`[billing] Callee payout FAILED to @${payment.callee}: ${result.reason}`);
      if (calleeSid) io.to(calleeSid).emit('payout-failed', { amount: calleeNet, reason: result.reason, callId });
    } else {
      ledgerPaymentUpdate(callId, 'payout', 'sent', result.txId);
    }
  } else {
    console.log(`[billing] Callee payout skipped (${calleeNet} HBD < 0.001 minimum)`);
  }

  // 2. Refund unused deposit to caller
  if (refundAmount >= 0.001) {
    const refundMemo = `v4call:refund:${callId}:unused-credit`;
    ledgerPayment(callId, 'refund', ESCROW_ACCOUNT, payment.caller, refundAmount, refundMemo, 'pending');
    const refundResult = await sendFromEscrow(payment.caller, refundAmount, refundMemo, 'HBD', callId);
    if (refundResult.success) {
      ledgerPaymentUpdate(callId, 'refund', 'sent', refundResult.txId);
      console.log(`[billing] Refunded ${refundAmount} HBD to @${payment.caller}`);
    } else {
      console.error(`[billing] Refund FAILED to @${payment.caller}: ${refundResult.reason}`);
      if (callerSid) io.to(callerSid).emit('payout-failed', {
        amount: refundAmount, reason: refundResult.reason, callId,
        message: 'Your unused credit refund could not be sent automatically'
      });
    }
  }

  // 3. Platform fee (ring fee + % cut of call earnings)
  if (platformTotal >= 0.001) {
    const feeMemo = `v4call:fee:${callId}:ring+cut`;
    ledgerPayment(callId, 'platform_fee', ESCROW_ACCOUNT, 'v4call', platformTotal, feeMemo, 'pending');
    const feeResult = await sendFromEscrow('v4call', platformTotal, feeMemo, 'HBD', callId);
    if (feeResult.success) {
      ledgerPaymentUpdate(callId, 'platform_fee', 'sent', feeResult.txId);
      console.log(`[billing] Platform fee sent: ${platformTotal} HBD`);
    } else {
      console.error(`[billing] Platform fee FAILED: ${feeResult.reason}`);
    }
  }

  delete activePayments[callId];
}

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
        const cutoff = Date.now() - (5 * 60 * 1000); // within last 5 minutes
        for (const [, op] of data.result) {
          if (op.op[0] !== 'transfer') continue;
          const t = op.op[1];
          if (t.from !== fromUser)           continue;
          if (t.to   !== toUser)             continue;
          if (!t.memo.includes(memo))        continue;
          const txTime   = new Date(op.timestamp + 'Z').getTime();
          if (txTime < cutoff)               continue;
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
    // Wait before retry (except on last attempt)
    if (attempt < retries) await new Promise(r => setTimeout(r, PAYMENT_VERIFY_DELAY_MS));
  }
  console.error(`[payment] ✗ Payment not found after ${retries} attempts`);
  return false;
}

// ── Tokens ────────────────────────────────────────────────────────────────────
const tokens = {};
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
setInterval(() => { const now = Date.now(); for (const k in tokens) if (tokens[k].expires < now) delete tokens[k]; }, 60000);

app.get('/join-token', (req, res) => {
  const t = consumeToken(req.query.token);
  if (!t) return res.status(403).json({ error: 'Invalid or expired token' });
  res.json({ username: t.username, pubKey: t.pubKey, roomName: t.roomName });
});

// ── Admin ledger endpoint (protected by secret key) ──────────────────────────
// Usage: GET /admin/ledger?key=YOUR_ADMIN_KEY&limit=50
// Usage: GET /admin/ledger/calls?key=YOUR_ADMIN_KEY&status=ended
// Set ADMIN_KEY env variable in your systemd service
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

// Escrow balance check endpoint
app.get('/admin/balance', async (req, res) => {
  const adminKey = process.env.ADMIN_KEY;
  if (!adminKey || req.query.key !== adminKey) return res.status(403).json({ error: 'Forbidden' });
  const balance = await getEscrowBalance();
  res.json({ account: ESCROW_ACCOUNT, balance_hbd: balance });
});

// ── Debug endpoint ────────────────────────────────────────────────────────────
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

// ── Debug: see exactly what rates were parsed for a user ─────────────────────
app.get('/debug-rates/:username', async (req, res) => {
  // Clear cache first so we always get fresh data
  delete rateCache[req.params.username];
  const rates = await fetchRates(req.params.username);
  if (!rates) {
    return res.json({
      found: false,
      message: `No v4call-rates post found for @${req.params.username}. Make sure the post exists with title exactly "v4call-rates" and contains a [V4CALL-RATES-V1] block.`
    });
  }
  // Also show what rates a specific caller would get right now
  const caller     = req.query.caller || 'unknown';
  const callType   = req.query.type   || 'voice';
  const applicable = getRatesForCaller(rates, caller, callType, new Date());
  res.json({ found: true, rates, applicable, testedWith: { caller, callType, time: new Date().toISOString() } });
});

// ── Rate endpoint (client can fetch callee rates before showing cost) ─────────
app.get('/rates/:username', async (req, res) => {
  const rates = await fetchRates(req.params.username);
  if (!rates) return res.json({ found: false });
  res.json({ found: true, rates });
});

// ─────────────────────────────────────────────────────────────────────────────

io.on('connection', (socket) => {
  console.log('Connected:', socket.id);

  // ── LOBBY ──────────────────────────────────────────────────────────────────

  socket.on('lobby-join', ({ username, pubKey }) => {
    socket._username  = username;
    socket._pubKey    = pubKey;
    socket._invisible = false;
    socket._room      = null;
    socket._pendingCall = null;

    const prev = lobbyUsers[username];
    lobbyUsers[username] = { socketId: socket.id, pubKey, invisible: prev ? prev.invisible : false };

    socket.emit('lobby-users',  lobbySnapshot());
    socket.emit('lobby-rooms',  roomsSnapshot());
    broadcastLobby();
    broadcastRooms();
    console.log(`@${username} entered lobby`);
  });

  socket.on('lobby-invisible', (invisible) => {
    const u = socket._username;
    if (!u || !lobbyUsers[u]) return;
    lobbyUsers[u].invisible = invisible;
    socket._invisible       = invisible;
    broadcastLobby();
  });

  // ── LOBBY CHAT ─────────────────────────────────────────────────────────────

  socket.on('lobby-chat', ({ message, signature, timestamp }) => {
    const from = socket._username;
    if (!from) return;
    io.emit('lobby-chat', { from, message, signature, timestamp });
  });

  socket.on('lobby-dm', ({ to, ciphertext, signature, timestamp }) => {
    const from = socket._username;
    if (!from) return;
    const recipient = lobbyUsers[to];
    if (!recipient) { socket.emit('lobby-dm-error', `@${to} is not online`); return; }
    io.to(recipient.socketId).emit('lobby-dm', { from, ciphertext, signature, timestamp });
    socket.emit('lobby-dm-sent', { to });
  });

  // ── RATE QUERY ─────────────────────────────────────────────────────────────
  // Client requests callee's rates before initiating a call
  socket.on('get-rates', async ({ callee, callType }, cb) => {
    const caller = socket._username;
    if (!caller || !callee || !callType) return cb({ error: 'Missing parameters' });

    const rates = await fetchRates(callee);
    if (!rates) {
      // No rates found — callee has no v4call-rates post, call is free
      return cb({ found: false, free: true });
    }

    const applicable = getRatesForCaller(rates, caller, callType, new Date());
    if (!applicable) {
      return cb({ found: true, free: true });
    }

    cb({ found: true, free: false, rates: applicable, escrow: rates.escrow || ESCROW_ACCOUNT });
  });

  // ── PAYMENT VERIFICATION ───────────────────────────────────────────────────
  // Caller tells server they've paid the ring fee — server verifies on blockchain
  socket.on('verify-ring-payment', async ({ callId, callee, amount, memo }, cb) => {
    const caller = socket._username;
    if (!caller) return cb({ verified: false, reason: 'Not authenticated' });

    console.log(`[payment] Verifying ring payment: @${caller} paid ${amount} HBD (memo: ${memo})`);
    const ok = await verifyHivePayment(caller, ESCROW_ACCOUNT, amount, memo);

    if (ok) {
      // Store payment record
      if (!activePayments[callId]) activePayments[callId] = {};
      activePayments[callId].ringPaid   = amount;
      activePayments[callId].ringMemo   = memo;
      activePayments[callId].caller     = caller;
      activePayments[callId].callee     = callee;
      // Fetch and store rate info for billing at call end
      const rates = await fetchRates(callee);
      if (rates) {
        const applicable = getRatesForCaller(rates, caller, 'voice', new Date());
        if (applicable) {
          activePayments[callId].ratePerHour  = applicable.rate       || 0;
          activePayments[callId].minDeposit   = applicable.minDeposit || 0;
          activePayments[callId].platformFee  = rates.platformFee     || 0.10;
        }
      }
      console.log(`[payment] Ring fee verified for call ${callId} — rate: ${activePayments[callId].ratePerHour} HBD/hr`);
    }

    cb({ verified: ok, reason: ok ? null : 'Payment not found on blockchain' });
  });

  // ── DIRECT CALLS ───────────────────────────────────────────────────────────

  // Verify combined deposit payment (ring + connect + credit deposit in one transfer)
  socket.on('verify-deposit-payment', async ({ callId, callee, totalAmount, depositAmount, connectAmount, memo }, cb) => {
    const caller = socket._username;
    if (!caller) return cb && cb({ verified: false, reason: 'Not authenticated' });

    console.log(`[payment] Verifying deposit: @${caller} paid ${totalAmount} HBD (memo: ${memo})`);
    const ok = await verifyHivePayment(caller, ESCROW_ACCOUNT, totalAmount, memo);

    if (ok) {
      if (!activePayments[callId]) activePayments[callId] = {};
      activePayments[callId].depositPaid     = depositAmount;   // credit portion (refundable)
      activePayments[callId].connectPaid     = connectAmount || 0; // connect fee (to callee)
      activePayments[callId].creditRemaining = depositAmount;
      activePayments[callId].caller          = caller;
      activePayments[callId].callee          = callee;
      console.log(`[payment] ✓ Deposit verified for call ${callId}: total=${totalAmount} connect=${connectAmount} deposit=${depositAmount} HBD`);
      ledgerPayment(callId, 'deposit', caller, ESCROW_ACCOUNT, totalAmount, memo, 'verified');
    }

    if (cb) cb({ verified: ok, reason: ok ? null : 'Payment not found on blockchain' });
  });

  // Add top-up credit during an active call
  socket.on('verify-topup-payment', async ({ callId, amount, memo }, cb) => {
    const caller = socket._username;
    if (!caller) return cb && cb({ verified: false });
    const ok = await verifyHivePayment(caller, ESCROW_ACCOUNT, amount, memo);
    if (ok && activePayments[callId]) {
      activePayments[callId].depositPaid     = (activePayments[callId].depositPaid || 0) + amount;
      activePayments[callId].creditRemaining = (activePayments[callId].creditRemaining || 0) + amount;
      const minLeft = activePayments[callId].creditRemaining / ((activePayments[callId].ratePerHour || 0) / 60);
      const room    = socket._room;
      if (room) io.to(room).emit('credit-topup', { amount, creditRemaining: activePayments[callId].creditRemaining, minutesLeft: minLeft });
      ledgerPayment(callId, 'topup', caller, ESCROW_ACCOUNT, amount, memo, 'verified');
      console.log(`[payment] Top-up ${amount} HBD for call ${callId}`);
    }
    if (cb) cb({ verified: ok });
  });

  // Verify connect fee payment (called after caller pays connect fee via Keychain)
  socket.on('verify-connect-payment', async ({ callId, callee, amount, memo }) => {
    const caller = socket._username;
    if (!caller) return;
    if (!activePayments[callId]) activePayments[callId] = {};
    const ok = await verifyHivePayment(caller, ESCROW_ACCOUNT, amount, memo);
    if (ok) {
      activePayments[callId].connectPaid = amount;
      console.log(`[payment] Connect fee verified for call ${callId}: ${amount} HBD`);
    }
  });

  // ── CALL END (explicit hang-up) ────────────────────────────────────────────
  socket.on('call-end', async ({ callId }) => {
    const username = socket._username;
    const room     = socket._room;
    if (room && rooms[room]) {
      const r = rooms[room];
      // Notify the other party
      socket.to(room).emit('peer-hung-up', { by: username });
      // Process billing
      const cid = r.callId || callId;
      if (cid) await processCallEnd(cid, room, io, lobbyUsers);
    }
    console.log(`📵 @${username} ended call`);
  });

  socket.on('call-user', ({ callee, callType, ringFeePaid, callId }) => {
    const caller = socket._username;
    if (!caller) {
      socket.emit('call-failed', { reason: 'Session not found — please refresh.' });
      return;
    }

    const getSocketId = (username) => lobbyUsers[username]?.socketId;

    // Rate limit — cooldown between calls to same callee
    if (ringFeePaid === 0 || !ringFeePaid) {
      // Only apply cooldown to free ring attempts, not paid ones
      // (paid ring attempts already have skin in the game)
      const cool = checkCallCooldown(caller, callee);
      if (!cool.allowed) {
        const waitSec = Math.ceil(cool.waitMs / 1000);
        socket.emit('call-failed', { reason: `Please wait ${waitSec}s before calling @${callee} again.` });
        return;
      }
    }

    if (!lobbyUsers[callee]) {
      // Callee went offline — refund ring fee if already paid
      if (ringFeePaid && ringFeePaid > 0 && callId) {
        const refundMemo = `v4call:refund:${callId}:offline`;
        ledgerPayment(callId, 'refund', ESCROW_ACCOUNT, caller, ringFeePaid, refundMemo, 'pending');
        sendFromEscrow(caller, ringFeePaid, refundMemo, 'HBD', callId).then(r => {
          if (r.success) ledgerPaymentUpdate(callId, 'refund', 'sent', r.txId);
          const msg = r.success
            ? `@${callee} is offline. Ring fee of ${ringFeePaid.toFixed(3)} HBD refunded.`
            : `@${callee} is offline. Refund of ${ringFeePaid.toFixed(3)} HBD pending — contact support.`;
          socket.emit('call-failed', { reason: msg, refunded: r.success });
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

    const roomName = `call__${caller}__${callee}__${Date.now()}`;

    const effectiveCallId = callId || roomName;
    rooms[roomName] = {
      creator:   caller,
      allowlist: new Set([caller, callee]),
      members:   [],
      createdAt: new Date(),
      isCall:    true,
      callType:  callType || 'voice',
      callId:    effectiveCallId
    };

    // Log call initiation to SQLite
    ledgerCallCreate(effectiveCallId, caller, callee, callType || 'voice');
    ledgerCallUpdate(effectiveCallId, { status: 'ringing' });
    if (ringFeePaid > 0) {
      ledgerPayment(effectiveCallId, 'ring', caller, ESCROW_ACCOUNT, ringFeePaid,
        `v4call:ring:${effectiveCallId}:${callee}`, 'verified');
    }

    if (lobbyUsers[caller]) lobbyUsers[caller].inCall = roomName;
    if (lobbyUsers[callee]) lobbyUsers[callee].inCall = roomName;

    socket._pendingCall = { roomName, callee };

    const calleeSid = getSocketId(callee);
    console.log(`📞 @${caller} → @${callee} (${callType}) room: ${roomName}`);
    console.log(`📞 callee socketId: ${calleeSid || 'NOT FOUND'}`);

    if (calleeSid) {
      io.to(calleeSid).emit('incoming-call', {
        caller,
        callerPubKey: socket._pubKey,
        roomName,
        callType: callType || 'voice',
        ringFeePaid: ringFeePaid || 0
      });
      console.log(`📞 incoming-call sent to @${callee}`);
    } else {
      socket.emit('call-failed', { reason: `@${callee} is online but unreachable. Ask them to refresh.` });
      delete rooms[roomName];
      if (lobbyUsers[caller]) delete lobbyUsers[caller].inCall;
      if (lobbyUsers[callee]) delete lobbyUsers[callee].inCall;
      return;
    }

    socket.emit('call-ringing', { callee, roomName });

    const timer = setTimeout(() => {
      if (rooms[roomName] && rooms[roomName].members.length === 0) {
        delete rooms[roomName];
        if (lobbyUsers[caller]) delete lobbyUsers[caller].inCall;
        if (lobbyUsers[callee]) delete lobbyUsers[callee].inCall;
        socket._pendingCall = null;
        socket.emit('call-missed', { callee, roomName });
        const calleeSidNow = getSocketId(callee);
        if (calleeSidNow) io.to(calleeSidNow).emit('call-missed', { caller, roomName });
        broadcastRooms();
        console.log(`⏰ Call timed out: @${caller} → @${callee}`);
      }
    }, 30000);

    rooms[roomName]._callTimer = timer;
    broadcastRooms();
  });

  socket.on('call-response', ({ roomName, accepted }) => {
    const callee = socket._username;
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
      // Log connection to SQLite
      ledgerCallUpdate(room.callId, { connected_at: new Date(now).toISOString(), status: 'connected' });
      // Start credit burn engine
      startCreditBurn(room.callId, roomName);
      // Duration cap timer
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
    if (room._callTimer) { clearTimeout(room._callTimer); delete room._callTimer; }
    delete rooms[roomName];
    if (lobbyUsers[caller]) delete lobbyUsers[caller].inCall;
    if (callee && lobbyUsers[callee]) delete lobbyUsers[callee].inCall;
    const calleeSid = lobbyUsers[callee]?.socketId;
    if (callee && calleeSid) io.to(calleeSid).emit('call-cancelled', { caller, roomName });
    socket._pendingCall = null;
    broadcastRooms();
    console.log(`🚫 @${caller} cancelled call to @${callee}`);
  });

  // ── ROOM CREATION ──────────────────────────────────────────────────────────

  socket.on('room-check', (roomName, cb) => { cb({ available: !rooms[roomName] }); });

  socket.on('room-create', ({ roomName, invitees }) => {
    const creator = socket._username;
    if (!creator) return;
    if (rooms[roomName]) { socket.emit('room-create-error', `Room "${roomName}" already exists.`); return; }
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

  // ── WEBRTC ─────────────────────────────────────────────────────────────────

  socket.on('offer',         ({ to, offer })      => { io.to(to).emit('offer',         { from: socket.id, offer }); });
  socket.on('answer',        ({ to, answer })     => { io.to(to).emit('answer',        { from: socket.id, answer }); });
  socket.on('ice-candidate', ({ to, candidate })  => { io.to(to).emit('ice-candidate', { from: socket.id, candidate }); });

  // ── ROOM CHAT ──────────────────────────────────────────────────────────────

  socket.on('chat-message', ({ room, to, from, ciphertext, broadcast, signature, timestamp }) => {
    if (broadcast) {
      socket.to(room).emit('chat-message', { from, to, ciphertext, broadcast: true, signature, timestamp });
    } else {
      if (!rooms[room]) return;
      const recipient = rooms[room].members.find(u => u.username === to);
      if (!recipient) return;
      io.to(recipient.socketId).emit('chat-message', { from, to, ciphertext, broadcast: false, signature, timestamp });
    }
  });

  // ── DISCONNECT ─────────────────────────────────────────────────────────────

  socket.on('disconnect', () => {
    const username = socket._username;
    const room     = socket._room;

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
      // If this was a call room, notify peer and process billing
      if (rooms[room].isCall) {
        socket.to(room).emit('peer-hung-up', { by: username });
        const callId = rooms[room].callId;
        if (callId && activePayments[callId]) {
          processCallEnd(callId, room, io, lobbyUsers);
        }
      }
      if (rooms[room].members.length === 0) {
        // Clear duration cap timers
        if (rooms[room]._capTimer)  clearTimeout(rooms[room]._capTimer);
        if (rooms[room]._warnTimer) clearTimeout(rooms[room]._warnTimer);
        delete rooms[room];
        console.log(`Room #${room} closed`);
      }
      broadcastRooms();
    }

    if (username && lobbyUsers[username]) {
      delete lobbyUsers[username];
      broadcastLobby();
      broadcastRooms();
    }

    console.log(`Disconnected: @${username || '?'} (${socket.id})`);
  });
});

server.listen(PORT, BIND_HOST, () => {
  console.log(`v4call server running on port ${PORT}`);

  // ── Startup checks ──────────────────────────────────────────────────────────
  const escrowKey = process.env.V4CALL_ESCROW_KEY;
  const adminKey  = process.env.ADMIN_KEY;

  if (!escrowKey) {
    console.error('⚠️  WARNING: V4CALL_ESCROW_KEY is not set — escrow payouts will not work!');
    console.error('   Add to /etc/systemd/system/webrtc.service: Environment=V4CALL_ESCROW_KEY=5K...');
  } else {
    // Verify the key is valid and matches escrow account
    try {
      const key    = dhive.PrivateKey.fromString(escrowKey);
      const pubKey = key.createPublic().toString();
      console.log(`✓ Escrow key loaded — public key: ${pubKey}`);

      // Verify key matches escrow account on-chain
      getEscrowBalance().then(balance => {
        console.log(`✓ Escrow account @${ESCROW_ACCOUNT} balance: ${balance.toFixed(3)} HBD`);
      }).catch(e => {
        console.error(`⚠️  Could not check escrow balance: ${e.message}`);
      });

      // Verify key matches the account's active key
      hivePost({
        jsonrpc: '2.0', method: 'condenser_api.get_accounts',
        params: [[ESCROW_ACCOUNT]], id: 1
      }).then(data => {
        if (!data?.result?.[0]) {
          console.error(`⚠️  Could not find @${ESCROW_ACCOUNT} on Hive`);
          return;
        }
        const activeKeys = data.result[0].active?.key_auths?.map(k => k[0]) || [];
        if (activeKeys.includes(pubKey)) {
          console.log(`✓ Escrow key verified — matches @${ESCROW_ACCOUNT} active key`);
        } else {
          console.error(`⚠️  WARNING: Escrow key does NOT match @${ESCROW_ACCOUNT} active key!`);
          console.error(`   Key provided derives to: ${pubKey}`);
          console.error(`   Account active keys: ${activeKeys.join(', ')}`);
          console.error('   Payouts will fail until correct active key is provided.');
        }
      }).catch(e => {
        console.error(`⚠️  Could not verify escrow key against blockchain: ${e.message}`);
      });

    } catch(e) {
      console.error(`⚠️  Invalid V4CALL_ESCROW_KEY format: ${e.message}`);
      console.error('   Make sure you are using the ACTIVE private key (starts with 5K or 5J)');
    }
  }

  if (!adminKey) {
    console.warn('⚠️  ADMIN_KEY not set — /admin/* endpoints are disabled');
  } else {
    console.log('✓ Admin key configured — /admin/ledger and /admin/balance available');
  }
});
