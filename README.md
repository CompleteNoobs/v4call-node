# v4call-node

The **headless v4call server** — CLI / API-only. Handles routing, presence, federation, chat, rooms, paid flows,
and the HTTP API (`/admin/*`, `/join-token`, `/api/info`, `/rates`). **Serves no HTML and holds no money key.**

- **Version:** 0.1.0
- **Federation protocol:** 0.4 (unchanged — a wire contract shared with deployed peers; 0.5 is reserved for
  federated paid expert invites, v0.17 Part B)
- Succeeds the monolith **v4call** (final version **v0.16.29**), carved out per the decoupling plan
  (`server.js` minus escrow, minus `express.static`).
- GUIs live in **v4call-app**; money lives in **escrow-core** deployments (e.g. `v4call-escrow`).
- **Source of truth:** [`../handover-decoupling.md`](../handover-decoupling.md)
- **Deploy guide:** `walkthrough.wiki` (bare-metal Ubuntu path + Docker variant)

> Status: **production** — live at `node.v4call.com` (client at `v4call.com`), settling real TEST-token
> sessions through the isolated escrow box.

## What's built beyond the monolith carve

- **Escrow split (`ESCROW_MODE=in-process|box`)** — in `box` mode the node is a **keyless reporter**: it
  publishes signed `event-report`s over escrow-protocol/0.1 (kind-31337 Nostr events via nGate) and finalizes
  on the box's signed `settlement-receipt`s. Durable single-winner report queue (`escrow-settlement-queue.js`),
  drainer with retry-until-received, terminal `failed` receipts end doomed retries, and **completion receipts**
  upgrade a settled-as-`pending` settlement once the box's recovery retry lands the payouts (users get a
  "✓ settlement completed" notice; a deferred paid-DM notify fires then too).
- **v0.17 Part A — paid expert invites (local, shipped + live-proven 2026-07-07).** The room admin offers
  payment to bring an expert into a room: 💎 offer-builder → cap escrowed up front via Keychain
  (`connectFee + maxDuration × rate`, memo `v4call:deposit:<offerId>`) → expert sees explicit terms →
  accept joins them as a 💎 paid member (`joinedVia:'paid'`) → live spend/earnings tickers → every exit
  settles like a metered call (payout→expert, unused refund→admin, fee→operator; envelope conserved).
  **Inviter-holds-funds** (rug-pull protection — inverts the 1:1-call treasurer direction). One offer at a
  time per inviter; each offer is its own contract; 30s disconnect grace; max-duration auto-end.
  Federated offers are **Part B**, deferred until a federation exists.

```
npm test    # node --test test/ — 55 passing (Node 20; on Node 24 use node --test test/*.test.js)
```
