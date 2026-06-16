# v4call-node

The **headless v4call server** — CLI / API-only. Handles routing, presence, federation, chat, and the HTTP API
(`/admin/*`, `/join-token`, `/api/info`, `/rates`). **Serves no HTML and holds no money key.**

- **Version:** 0.1.0
- **Federation protocol:** 0.4 (unchanged — a wire contract shared with deployed peers)
- Succeeds the monolith **v4call** (final version **v0.16.29**), carved out per the decoupling plan
  (`server.js` minus escrow, minus `express.static`).
- GUIs live in **v4call-app**; money lives in **escrow-core** deployments (e.g. `v4call-escrow`).
- **Source of truth:** [`../handover-decoupling.md`](../handover-decoupling.md)

> Status: scaffold. Code carved over during the decoupling build (see the hand-off doc, build sequence §11).
