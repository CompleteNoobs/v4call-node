# ── v4call-node Dockerfile (headless) ────────────────────────────────────────
FROM node:20-alpine

# Install build tools needed for better-sqlite3 native module + curl for the
# /admin/discovery-test diagnostic endpoint (alpine ships wget but not curl).
RUN apk add --no-cache python3 make g++ sqlite curl

WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev

COPY server.js ./
COPY nostr-fed.mjs ./
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
