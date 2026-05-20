// ───────────────────────────────────────────────────────────────────────────
// nostr-fed.mjs — v4call Nostr Federation, PHASE B (the real server publishes)
//
// WHAT THIS DOES
//   • On boot: load (or create) this server's own Nostr identity key.
//   • Publish a kind-30078 "I am a v4call server" announce to the relays.
//   • Re-publish every NOSTR_REPUBLISH_HOURS (relays drop events; repeat).
//   • Respect FED_DISCOVERY_MODE: only publishes when mode is nostr|both.
//
// WHAT THIS DOES *NOT* DO (later phases)
//   • Does NOT subscribe / listen for other servers   → Phase C
//   • Does NOT feed the peer-approval list             → Phase C
//   • Does NOT do presence ("user X online")           → Phase D
//   • Does NOT touch the existing WS federation, money, DMs, calls — ever.
//
// WHY ESM (.mjs): nostr-tools v2 is ESM-only. server.js is CommonJS and loads
// this via dynamic import(). The boundary is deliberately tiny.
//
// SAFETY: every path is wrapped so a Nostr problem can NEVER crash v4call.
//   The server runs perfectly fine on Hive-only discovery if this misbehaves.
// ───────────────────────────────────────────────────────────────────────────

import { generateSecretKey, getPublicKey, finalizeEvent } from 'nostr-tools/pure';
import { npubEncode, decode as nip19decode } from 'nostr-tools/nip19';
import { SimplePool, useWebSocketImplementation } from 'nostr-tools/pool';
import WebSocket from 'ws';
import { webcrypto } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';

// Node 18 doesn't expose Web Crypto globally; Node 20 (prod) does. Harmless
// either way — only fills the gap if it's missing.
if (!globalThis.crypto) globalThis.crypto = webcrypto;
useWebSocketImplementation(WebSocket);

const LOG = (...a) => console.log('[nostr]', ...a);
const ERR = (...a) => console.error('[nostr]', ...a);

// ── Key bootstrap ──────────────────────────────────────────────────────────
// Priority: existing key file  →  one-time NOSTR_NSEC seed  →  auto-generate.
// The file is the long-lived identity. It MUST sit on a mounted volume (see
// docker-compose ./data/nostr) or it is lost on every container rebuild.
function loadOrCreateKey(keyPath, nsecSeed) {
  // 1. Existing key file wins, always.
  if (fs.existsSync(keyPath)) {
    const j = JSON.parse(fs.readFileSync(keyPath, 'utf8'));
    const skHex = j.sk_hex;
    const pkHex = getPublicKey(hexToBytes(skHex));
    LOG(`loaded existing identity from ${keyPath}`);
    return { skBytes: hexToBytes(skHex), pkHex, npub: npubEncode(pkHex), source: 'file' };
  }

  // Ensure the key directory exists before we try to write into it.
  fs.mkdirSync(path.dirname(keyPath), { recursive: true });

  // 2. One-time seed from an existing nsec the operator pasted into .env.
  if (nsecSeed && nsecSeed.trim()) {
    let skBytes;
    try {
      const dec = nip19decode(nsecSeed.trim());
      if (dec.type !== 'nsec') throw new Error('not an nsec');
      skBytes = dec.data;
    } catch (e) {
      throw new Error(`NOSTR_NSEC is not a valid nsec key: ${e.message}`);
    }
    const pkHex = getPublicKey(skBytes);
    writeKeyFile(keyPath, bytesToHex(skBytes), npubEncode(pkHex));
    LOG(`seeded identity from NOSTR_NSEC → wrote ${keyPath}`);
    LOG(`you can now REMOVE NOSTR_NSEC from .env (the file is the identity now)`);
    return { skBytes, pkHex, npub: npubEncode(pkHex), source: 'seed' };
  }

  // 3. Brand-new identity.
  const skBytes = generateSecretKey();
  const pkHex = getPublicKey(skBytes);
  writeKeyFile(keyPath, bytesToHex(skBytes), npubEncode(pkHex));
  LOG(`generated a NEW Nostr identity → wrote ${keyPath}`);
  return { skBytes, pkHex, npub: npubEncode(pkHex), source: 'generated' };
}

function writeKeyFile(keyPath, skHex, npub) {
  fs.writeFileSync(
    keyPath,
    JSON.stringify({ sk_hex: skHex, npub, created_at: new Date().toISOString() }, null, 2),
    { mode: 0o600 }
  );
  try { fs.chmodSync(keyPath, 0o600); } catch { /* best effort */ }
}

// nostr-tools wants Uint8Array secret keys; we store hex in the file.
function hexToBytes(hex) {
  const a = new Uint8Array(hex.length / 2);
  for (let i = 0; i < a.length; i++) a[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  return a;
}
function bytesToHex(bytes) {
  return [...bytes].map(b => b.toString(16).padStart(2, '0')).join('');
}

// ── Build + publish the announce event ─────────────────────────────────────
function buildAnnounce(cfg, skBytes) {
  const unsigned = {
    kind: 30078,                                  // NIP-78 app data, replaceable
    created_at: Math.floor(Date.now() / 1000),
    tags: [
      ['d', cfg.domain],                          // replaceable key = our domain
      ['t', 'v4call-server'],                      // discovery tag peers filter on
      ['protocol', cfg.protocol],
    ],
    content: JSON.stringify({
      verify_url: cfg.verifyUrl,
      hive_account: cfg.hiveAccount,
      domain: cfg.domain,
      announced_at: new Date().toISOString(),
    }),
  };
  return finalizeEvent(unsigned, skBytes);
}

async function publishOnce(pool, relays, ev) {
  // Three-state detection, learned in Phase A: a relay that is DOWN resolves
  // (does not throw) with "connection failure: ...". A real OK resolves. A
  // rejection (e.g. nGate "blocked") throws. Treat all three distinctly so a
  // down peer relay never looks like a rejection.
  const settled = await Promise.allSettled(pool.publish(relays, ev));
  settled.forEach((r, i) => {
    if (r.status === 'rejected') {
      // Expected until nGate whitelists us: "blocked: not on relay whitelist".
      LOG(`  ✗ ${relays[i]} REJECTED: ${r.reason}`);
    } else if (typeof r.value === 'string' && r.value.startsWith('connection failure')) {
      LOG(`  … ${relays[i]} UNREACHABLE (relay not answering)`);
    } else {
      LOG(`  ✓ ${relays[i]} ACCEPTED${r.value ? ' — ' + r.value : ''}`);
    }
  });
}

// ── Phase C: long-lived subscription for other servers' announces ──────────
// Filter: kind 30078 + #t=v4call-server. Newest-per-domain (`d` tag) wins
// (kind 30078 is replaceable, so older events for the same `d` are stale).
// We dedupe by event id (relays may deliver the same event N times) and
// also by domain-with-newer-timestamp so an out-of-order arrival doesn't
// downgrade a fresher view we already have.
function startSubscribe(pool, cfg, ownPkHex) {
  const relays = cfg.relays;
  const seenEventIds  = new Set();
  const newestByDomain = new Map();        // domain → created_at seconds
  let resubCount = 0;

  const filter = { kinds: [30078], '#t': ['v4call-server'] };

  const onevent = (ev) => {
    try {
      if (seenEventIds.has(ev.id)) return;
      seenEventIds.add(ev.id);

      // Skip our own announce (we wrote it).
      if (ev.pubkey === ownPkHex) return;

      const dTag = ev.tags.find(t => t[0] === 'd');
      const domain = dTag && dTag[1] ? String(dTag[1]).toLowerCase() : null;
      if (!domain) return;

      // Skip events claiming our own domain (we'd never need to discover us).
      if (cfg.ownDomain && domain === String(cfg.ownDomain).toLowerCase()) return;

      // Newer-wins per domain (kind 30078 is replaceable — but a relay can
      // still hand us older copies).
      const prevTs = newestByDomain.get(domain) || 0;
      if (ev.created_at <= prevTs) return;
      newestByDomain.set(domain, ev.created_at);

      // Display-only content — must NEVER be trusted for security. The
      // callback re-fetches /.well-known/v4call-server.json and uses the
      // Hive-signed values for anything that matters.
      let content = null;
      try { content = JSON.parse(ev.content); } catch { /* best effort */ }

      LOG(`◀ event from relay: domain=${domain} pubkey=${ev.pubkey.slice(0,12)}… ts=${ev.created_at} — handing to discovery`);
      // Fire-and-forget; the callback handles its own errors.
      Promise.resolve(cfg.onDiscover({
        domain,
        pubkey: ev.pubkey,
        eventId: ev.id,
        content,
      })).catch(e => ERR(`onDiscover threw (non-fatal): ${e.message}`));
    } catch (e) {
      ERR(`subscribe onevent error (non-fatal): ${e.message}`);
    }
  };

  const oneose = () => {
    LOG(`subscribe: relay(s) sent stored backlog — now live`);
  };

  // SimplePool re-subscribes automatically when a relay reconnects, but a
  // periodic re-open is cheap belt-and-braces against silent half-open
  // sockets. Every ~30 min we tear down and re-open the subscription.
  const openSub = () => {
    resubCount++;
    LOG(`subscribe: opening (#${resubCount}) on ${relays.length} relay(s), filter t=v4call-server kind=30078`);
    return pool.subscribeMany(relays, filter, { onevent, oneose });
  };
  let sub = openSub();
  setInterval(() => {
    try { sub.close(); } catch { /* */ }
    sub = openSub();
  }, 30 * 60 * 1000).unref();
}

// ── Public entry point — called once from server.js at startup ─────────────
export async function startNostrFed(cfg) {
  try {
    const { skBytes, pkHex, npub, source } = loadOrCreateKey(cfg.keyPath, cfg.nsecSeed);

    LOG(`identity: ${npub}`);
    LOG(`identity hex: ${pkHex}`);
    LOG(`────────────────────────────────────────────────────────────────`);
    LOG(`ACTION NEEDED (one time): put this line in your Hive server-announce`);
    LOG(`post via /server-announce.html, then re-publish it:`);
    LOG(`    NOSTR_PUBKEY:${npub}`);
    LOG(`Until nGate scans Hive and whitelists this key, gated relays will`);
    LOG(`REJECT our publishes with "blocked" — that is expected and self-heals.`);
    LOG(`────────────────────────────────────────────────────────────────`);

    if (cfg.mode === 'hive') {
      LOG(`FED_DISCOVERY_MODE=hive — Nostr publishing is OFF (identity ready`);
      LOG(`for when you switch to 'both' or 'nostr'). Nothing else to do.`);
      return;
    }

    if (!cfg.relays.length) {
      ERR(`no relays configured (NOSTR_RELAYS empty) — nothing to publish to`);
      return;
    }

    const pool = new SimplePool();
    const doPublish = async () => {
      try {
        const ev = buildAnnounce(cfg, skBytes);
        LOG(`publishing announce for ${cfg.domain} (event ${ev.id.slice(0, 12)}…) to ${cfg.relays.length} relay(s)`);
        await publishOnce(pool, cfg.relays, ev);
      } catch (e) {
        ERR(`publish cycle failed (non-fatal): ${e.message}`);
      }
    };

    await doPublish();                              // on boot
    const everyMs = Math.max(1, cfg.republishHours) * 60 * 60 * 1000;
    setInterval(doPublish, everyMs).unref();         // repeat; don't hold the process open
    LOG(`will re-announce every ${cfg.republishHours}h (mode=${cfg.mode}, key source=${source})`);

    // ── Phase C: subscribe for other servers' announces ─────────────────────
    // We trust NOTHING in incoming events here — we extract only the domain
    // (and a few display-only fields) and hand it to onDiscover(). The
    // server.js callback runs the real Hive-anchored verifyPeer() before
    // anything lands in discoveredPeers. nostr-fed.mjs stays a dumb transport.
    if (cfg.subscribeEnabled && typeof cfg.onDiscover === 'function') {
      startSubscribe(pool, cfg, pkHex);
    } else if (cfg.subscribeEnabled) {
      LOG(`subscribe requested but no onDiscover callback wired — skipping`);
    }
  } catch (e) {
    // Absolute backstop: Nostr must NEVER take down v4call.
    ERR(`startup failed (v4call continues normally on Hive-only): ${e.message}`);
  }
}
