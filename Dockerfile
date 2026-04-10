# ── v4call Dockerfile ─────────────────────────────────────────────────────────
FROM node:20-alpine

# Install build tools needed for better-sqlite3 native module
RUN apk add --no-cache python3 make g++ sqlite

WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev

COPY server.js ./
COPY public/ ./public/

# Create logs directory and give the node user ownership
# This must happen before switching to USER node
RUN mkdir -p /app/logs && chown -R node:node /app/logs

VOLUME ["/app/logs"]

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
  CMD wget -qO- http://localhost:3000/debug-state || exit 1

USER node

CMD ["node", "server.js"]
