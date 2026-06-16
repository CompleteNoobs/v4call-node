// ───────────────────────────────────────────────────────────────────────────
// nostr-fed.mjs — v4call Nostr Federation, PHASE B (the real server publishes)
//
// WHAT THIS DOES
//   • On boot: load (or create) this server's own Nostr identity key.
//   • Publish a kind-30078 "I am a v4call server" announce to the relays.
//   • Re-publish every NOSTR_REPUBLISH_HOURS (relays drop events; repeat).
//   • Respect FED_DISCOVERY_MODE: only publishes when mode is nostr|both.
//
// NOTE: header below describes the original Phase-B scope. Phases C (subscribe +
// discovery), D (presence), and the optional payload transport (dm + dm-attachment
// over Nostr, NIP-44-encrypted) have since shipped — see startSubscribe /
// startPresence / startFedTransport. The module is still a DUMB TRANSPORT:
// server.js owns every trust decision (Hive-anchored pubkey↔domain binding,
// approved-peer gate). It decrypts (only it holds the key) but never decides who
// to trust, and a Nostr problem must NEVER crash v4call.
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
import * as nip44 from 'nostr-tools/nip44';
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

// ── Payload transport (optional layer on top of discovery/presence) ─────────
// Carries the existing WS-federation envelopes (dm + dm-attachment) over Nostr
// relays when the WS /federation transport is down/disabled. NIP-44-encrypted
// server→peer so a public relay only sees an opaque blob; server.js owns the
// approved-peer trust gate (we stay a dumb transport here).
const FEDMSG_KIND = 1314;              // regular/stored kind → relays keep it for
                                       // store-and-forward backlog (NOT replaceable
                                       // 30078, NOT ephemeral 2xxxx).
const FEDMSG_TAG  = 'v4call-fedmsg';   // discriminator so this never collides with
                                       // the v4call-server / v4call-presence subs.
const FEDMSG_MAX_PLAINTEXT = 49152;    // ~48KB safety margin under NIP-44's 65535
                                       // cap. The attachment "wrapper" is small
                                       // metadata (CID + wrapped keys) — file bytes
                                       // live on IPFS — so this is generous headroom.

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

// ── Phase D: presence publish + subscribe ──────────────────────────────────
// Returns { notePresenceChange } — call notePresenceChange whenever local
// users join/leave; we'll publish a throttled snapshot. Heartbeat republishes
// every cfg.presenceHeartbeatMs regardless, so a relay drop self-heals.
//
// Event shape (NIP-78 replaceable, d-tag = `<domain>:users` so it doesn't
// collide with the discovery event's d=<domain>):
//   { kind:30078, tags:[["d",`${domain}:users`],["t","v4call-presence"]],
//     content: {"domain":"<domain>","users":[...], "updated_at":"..."} }
//
// Trust gate is NOT in this module — server.js's onPresence runs the
// Hive-anchored binding check before anything lands in cross-fed state.
function startPresence(pool, cfg, skBytes, ownPkHex) {
  const throttleMs  = Math.max(1000, cfg.presenceThrottleMs);
  const heartbeatMs = Math.max(throttleMs, cfg.presenceHeartbeatMs);

  let lastPublishAt   = 0;
  let pendingTimer    = null;
  let lastSentJoined  = '';

  const buildPresenceEvent = (users) => {
    const unsigned = {
      kind: 30078,
      created_at: Math.floor(Date.now() / 1000),
      tags: [
        ['d', `${cfg.domain}:users`],
        ['t', 'v4call-presence'],
        ['protocol', cfg.protocol],
      ],
      content: JSON.stringify({
        domain:     cfg.domain,
        users,                                   // already sorted by server.js
        updated_at: new Date().toISOString(),
      }),
    };
    return finalizeEvent(unsigned, skBytes);
  };

  const doPublish = async (reason) => {
    try {
      const users = (cfg.getLocalUsers() || []).map(u => String(u).toLowerCase()).sort();
      const joined = users.join(',');
      // If nothing changed AND this isn't a heartbeat, skip. Heartbeat always
      // publishes (covers relay drops + lets peers prove we're alive).
      if (reason !== 'heartbeat' && joined === lastSentJoined) return;
      lastSentJoined = joined;
      lastPublishAt  = Date.now();
      const ev = buildPresenceEvent(users);
      LOG(`presence publish (${reason}): ${users.length} local user(s) for ${cfg.domain} — event ${ev.id.slice(0, 12)}…`);
      await publishOnce(pool, cfg.relays, ev);
    } catch (e) {
      ERR(`presence publish failed (non-fatal): ${e.message}`);
    }
  };

  // Initial publish so peers know we're alive (even if user list is empty).
  doPublish('boot');

  // Heartbeat — always republish at this cadence regardless of change.
  setInterval(() => doPublish('heartbeat'), heartbeatMs).unref();

  // Throttle: on a change, publish immediately if we're past the window,
  // else schedule a single delayed publish at window-end. Coalesces bursts.
  const notePresenceChange = () => {
    const sinceLast = Date.now() - lastPublishAt;
    if (sinceLast >= throttleMs) {
      doPublish('change');
    } else if (!pendingTimer) {
      pendingTimer = setTimeout(() => {
        pendingTimer = null;
        doPublish('change-delayed');
      }, throttleMs - sinceLast);
      pendingTimer.unref?.();
    }
    // If a timer is already pending, additional changes just ride along.
  };

  // Subscribe — listen for OTHER servers' presence snapshots.
  // Filter intentionally narrow (only the presence tag) so server-discovery
  // events from the same kind don't get processed twice.
  const filter = { kinds: [30078], '#t': ['v4call-presence'] };
  const seenIds = new Set();
  let resubCount = 0;

  const onevent = (ev) => {
    try {
      if (seenIds.has(ev.id)) return;
      seenIds.add(ev.id);
      if (ev.pubkey === ownPkHex) return;             // never our own
      // Extract domain from `d` tag (`<domain>:users` → strip suffix).
      const dTag = ev.tags.find(t => t[0] === 'd');
      const dVal = dTag && dTag[1] ? String(dTag[1]).toLowerCase() : '';
      const m    = /^(.+):users$/.exec(dVal);
      if (!m) return;                                 // not a presence d-tag shape
      const domainFromTag = m[1];
      if (cfg.ownDomain && domainFromTag === String(cfg.ownDomain).toLowerCase()) return;

      let content = null;
      try { content = JSON.parse(ev.content); } catch { return; }
      if (!content || typeof content !== 'object') return;
      const domainInBody = String(content.domain || '').toLowerCase();
      // Cross-check d-tag vs body — drop on mismatch (cheap forgery signal).
      if (domainInBody !== domainFromTag) return;
      const users = Array.isArray(content.users) ? content.users : [];

      // Hand to server.js for the real Hive-anchored trust check.
      Promise.resolve(cfg.onPresence({
        domain:  domainFromTag,
        users,
        pubkey:  ev.pubkey,
        eventId: ev.id,
        ts:      ev.created_at,
      })).catch(e => ERR(`onPresence threw (non-fatal): ${e.message}`));
    } catch (e) {
      ERR(`presence onevent error (non-fatal): ${e.message}`);
    }
  };

  const openSub = () => {
    resubCount++;
    LOG(`presence subscribe: opening (#${resubCount}) on ${cfg.relays.length} relay(s)`);
    return pool.subscribeMany(cfg.relays, filter, {
      onevent,
      oneose: () => LOG(`presence subscribe: relay(s) sent backlog — now live`),
    });
  };
  let sub = openSub();
  setInterval(() => {
    try { sub.close(); } catch { /* */ }
    sub = openSub();
  }, 30 * 60 * 1000).unref();

  return { notePresenceChange };
}

// ── Payload transport: publish one encrypted fedmsg to a peer ──────────────
// NIP-44-encrypt the inner federation envelope to the peer's pubkey, wrap in a
// regular (stored) kind-1314 event tagged for them, NIP-40 expiration so relays
// GC delivered messages. Returns { ok, eventId } | { ok:false, reason }.
async function nostrFedSendImpl(pool, relays, skBytes, ownPkHex, peerHex, msgObj, ttlSeconds) {
  try {
    if (!peerHex || typeof peerHex !== 'string') return { ok: false, reason: 'no peer pubkey' };
    if (!msgObj || typeof msgObj !== 'object')    return { ok: false, reason: 'no message' };
    const plaintext = JSON.stringify(msgObj);
    if (plaintext.length > FEDMSG_MAX_PLAINTEXT) {
      ERR(`fedmsg too large (${plaintext.length}B > ${FEDMSG_MAX_PLAINTEXT}) — refusing Nostr route (use WS)`);
      return { ok: false, reason: 'too large for nostr transport' };
    }
    const convKey = nip44.getConversationKey(skBytes, peerHex);
    const ct      = nip44.encrypt(plaintext, convKey);
    const now     = Math.floor(Date.now() / 1000);
    const ttl     = Math.max(60, ttlSeconds || 86400);
    const ev = finalizeEvent({
      kind: FEDMSG_KIND,
      created_at: now,
      tags: [
        ['p', peerHex],
        ['t', FEDMSG_TAG],
        ['expiration', String(now + ttl)],
      ],
      content: ct,
    }, skBytes);
    LOG(`fedmsg → ${peerHex.slice(0, 12)}… type=${msgObj.type} (event ${ev.id.slice(0, 12)}…)`);
    await publishOnce(pool, relays, ev);
    return { ok: true, eventId: ev.id };
  } catch (e) {
    ERR(`fedmsg publish failed (non-fatal): ${e.message}`);
    return { ok: false, reason: e.message };
  }
}

// ── Payload transport: subscribe for fedmsgs addressed to us ────────────────
// Decrypt (only we hold the key), parse, hand the inner envelope up to
// cfg.onFedMessage. server.js resolves pubkey→approved-domain, builds a
// pseudo-socket, and runs the SAME fedHandleMessage dispatcher the WS path
// uses — so dm / dm-attachment handlers (incl. recipient-side rate enforcement
// + refunds) run unchanged. We make NO trust decision here.
function startFedTransport(pool, cfg, skBytes, ownPkHex) {
  const relays  = cfg.relays;
  const seenIds = new Set();
  let resubCount = 0;
  const filter = { kinds: [FEDMSG_KIND], '#p': [ownPkHex], '#t': [FEDMSG_TAG] };

  const onevent = (ev) => {
    try {
      if (seenIds.has(ev.id)) return;
      seenIds.add(ev.id);
      // Cheap unbounded-growth guard; server.js holds the authoritative
      // time-windowed dedup, so a hard reset here is harmless belt-and-braces.
      if (seenIds.size > 5000) seenIds.clear();
      if (ev.pubkey === ownPkHex) return;            // never our own

      let inner = null;
      try {
        const convKey = nip44.getConversationKey(skBytes, ev.pubkey);
        inner = JSON.parse(nip44.decrypt(ev.content, convKey));
      } catch {
        return;                                       // wrong key / not for us / malformed → drop quietly
      }
      if (!inner || typeof inner !== 'object' || !inner.type) return;

      LOG(`fedmsg ◀ from ${ev.pubkey.slice(0, 12)}… type=${inner.type} (event ${ev.id.slice(0, 12)}…)`);
      Promise.resolve(cfg.onFedMessage({ fromPubkey: ev.pubkey, eventId: ev.id, innerMsg: inner }))
        .catch(e => ERR(`onFedMessage threw (non-fatal): ${e.message}`));
    } catch (e) {
      ERR(`fedmsg onevent error (non-fatal): ${e.message}`);
    }
  };

  const openSub = () => {
    resubCount++;
    LOG(`fedmsg subscribe: opening (#${resubCount}) on ${relays.length} relay(s), filter kind=${FEDMSG_KIND} #p=self`);
    return pool.subscribeMany(relays, filter, {
      onevent,
      oneose: () => LOG(`fedmsg subscribe: relay backlog received — now live (store-and-forward replay done)`),
    });
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

    // ── Phase D: presence (publish + subscribe) ─────────────────────────────
    // Publish: a throttled + heartbeat'd snapshot of our LOCAL online users.
    // Subscribe: receive other servers' snapshots; hand to cfg.onPresence
    // which is server.js's recordNostrPresence (the real trust gate). Same
    // "dumb transport, server.js owns trust" rule as Phase C.
    let notePresenceChange = () => {};
    if (cfg.presenceEnabled && typeof cfg.getLocalUsers === 'function' &&
        typeof cfg.onPresence === 'function') {
      const ctl = startPresence(pool, cfg, skBytes, pkHex);
      notePresenceChange = ctl.notePresenceChange;
      LOG(`presence enabled — throttle ${cfg.presenceThrottleMs/1000}s, heartbeat ${cfg.presenceHeartbeatMs/1000}s`);
    } else if (cfg.presenceEnabled) {
      LOG(`presence requested but missing getLocalUsers/onPresence — skipping`);
    }

    // ── Payload transport (optional) — carry dm/dm-attachment over Nostr ────
    // Same "dumb transport, server.js owns trust" split as Phase C/D: we
    // subscribe + decrypt; server.js's onFedMessage does the approved-peer gate
    // and dispatches into the existing fedHandleMessage.
    if (cfg.fedTransportEnabled && typeof cfg.onFedMessage === 'function') {
      startFedTransport(pool, cfg, skBytes, pkHex);
      LOG(`fedmsg transport ENABLED — dm + dm-attachment payloads can route over Nostr (kind ${FEDMSG_KIND}, ttl ${cfg.fedmsgTtlSeconds || 86400}s)`);
    } else if (cfg.fedTransportEnabled) {
      LOG(`fedmsg transport requested but no onFedMessage callback wired — skipping`);
    }

    // Bound send helper for server.js (publishes one encrypted fedmsg to a peer
    // pubkey). Available only once the pool exists; server.js guards on it.
    const nostrFedSend = (peerHex, msgObj) =>
      nostrFedSendImpl(pool, cfg.relays, skBytes, pkHex, peerHex, msgObj, cfg.fedmsgTtlSeconds);

    // Return a controller so server.js can drive event-triggered publishes.
    // notePresenceChange is a no-op when presence is off, so callers don't
    // need to gate. nostrFedSend routes a payload envelope over Nostr.
    return { notePresenceChange, nostrFedSend };
  } catch (e) {
    // Absolute backstop: Nostr must NEVER take down v4call.
    ERR(`startup failed (v4call continues normally on Hive-only): ${e.message}`);
  }
}
