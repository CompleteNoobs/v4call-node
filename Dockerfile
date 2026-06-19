# ── v4call-node Dockerfile (headless) ────────────────────────────────────────
FROM node:20-alpine

# Install build tools needed for better-sqlite3 native module + curl for the
# /admin/discovery-test diagnostic endpoint (alpine ships wget but not curl).
RUN apk add --no-cache python3 make g++ sqlite curl

WORKDIR /app

COPY package.json ./

# escrow-core is a sibling dependency (`file:../escrow-core` in package.json) and
# lives OUTSIDE this build context. `npm run docker:prep` vendors a clean source
# snapshot into ./vendor/escrow-core; we place it at /escrow-core — a sibling of
# /app — so the UNCHANGED `file:../escrow-core` path resolves at install time
# exactly as on bare metal.
COPY vendor/escrow-core /escrow-core

# --install-links is REQUIRED here: it packs the file: dependency into a real
# directory in node_modules instead of a symlink. A symlink would make Node
# resolve escrow-core's own deps (@hiveio/dhive, @noble/curves, better-sqlite3)
# from the symlink's realpath (/escrow-core) — which has no node_modules in the
# image — and the node would crash at boot with "Cannot find module '@hiveio/dhive'".
# (Bare metal avoids this by installing escrow-core independently; the container
# can't.) escrow-core's better-sqlite3 (^11) rebuilds for the image arch alongside
# v4call-node's (^9) — two native compile passes is expected.
RUN npm install --omit=dev --install-links

# All root JS/MJS sources (server.js + its local requires: escrow-verify.js,
# escrow-report.js, nostr-fed.mjs). Globbed deliberately so adding a new local
# module can't silently break the image the way an explicit list did. The build
# context excludes test/ and node_modules via .dockerignore, so only the app
# sources match here.
COPY *.js *.mjs ./
# Vendored node↔app shared module(s) required by server.js (./shared/v4call-wellknown).
# Without this the image boots-crash with "Cannot find module './shared/...'".
COPY shared ./shared
# Headless: no public/ (GUIs live in v4call-app). The signed domain-proof file is
# mounted at runtime via V4CALL_SERVER_JSON_PATH (default /app/data/v4call-server.json),
# not baked into the image.

# Create logs + nostr-key + data directories and give the node user ownership.
# This must happen before switching to USER node. (Bind-mounts from the host may
# still be root-owned — if you see "EACCES", run `chown -R 1000:1000 ./data` on host.)
RUN mkdir -p /app/logs /app/nostr /app/data && chown -R node:node /app/logs /app/nostr /app/data

VOLUME ["/app/logs", "/app/nostr", "/app/data"]

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
  CMD wget -qO- http://localhost:3000/api/info || exit 1

USER node

CMD ["node", "server.js"]
