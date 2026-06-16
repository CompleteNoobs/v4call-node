# ── v4call Dockerfile ─────────────────────────────────────────────────────────
FROM node:20-alpine

# Install build tools needed for better-sqlite3 native module + curl for the
# /admin/discovery-test diagnostic endpoint (alpine ships wget but not curl).
RUN apk add --no-cache python3 make g++ sqlite curl

WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev

COPY server.js ./
COPY nostr-fed.mjs ./
COPY public/ ./public/

# Create logs + nostr-key directories and give the node user ownership.
# This must happen before switching to USER node. (The /app/nostr bind-mount
# from the host may still be root-owned — see WalkThrough: if you see
# "EACCES ... nostr-key.json", run `chown -R 1000:1000 ./data/nostr` on host.)
RUN mkdir -p /app/logs /app/nostr && chown -R node:node /app/logs /app/nostr

VOLUME ["/app/logs", "/app/nostr"]

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
  CMD wget -qO- http://localhost:3000/api/info || exit 1

USER node

CMD ["node", "server.js"]
