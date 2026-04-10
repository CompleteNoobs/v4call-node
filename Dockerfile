# ── v4call Dockerfile ─────────────────────────────────────────────────────────
# Builds a self-contained v4call server image.
# Uses Node.js 20 LTS on Alpine Linux for a small image size (~150MB).

FROM node:20-alpine

# Install build tools needed for better-sqlite3 native module
RUN apk add --no-cache python3 make g++ sqlite

# Create app directory
WORKDIR /app

# Copy package.json first (Docker layer caching — only re-runs npm install
# if package.json changes, not on every code change)
COPY package.json ./

# Install dependencies
# Using npm install instead of npm ci — ci requires a package-lock.json
RUN npm install --omit=dev

# Copy application source
COPY server.js ./
COPY public/ ./public/

# Create logs directory for SQLite database
RUN mkdir -p /app/logs

# The app writes its SQLite database to /app/logs/v4call-ledger.db
# Mount this as a volume to persist data across container restarts
VOLUME ["/app/logs"]

# Expose the internal port (default 3000, configurable via PORT env var)
EXPOSE 3000

# Health check — verifies the server is responding
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
  CMD wget -qO- http://localhost:3000/debug-state || exit 1

# Run as non-root user for security
USER node

# Start the server
CMD ["node", "server.js"]
