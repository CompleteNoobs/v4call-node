# ── v4call Dockerfile ─────────────────────────────────────────────────────────
# Builds a self-contained v4call server image.
# Uses Node.js 20 LTS on Alpine Linux for a small image size (~150MB).

FROM node:20-alpine

# Install build tools needed for better-sqlite3 native module
RUN apk add --no-cache python3 make g++ sqlite

# Create app directory
WORKDIR /app

# Copy package files first (Docker layer caching — only re-runs npm install
# if package.json or package-lock.json change, not on every code change)
COPY package*.json ./

# Install dependencies
RUN npm ci --omit=dev

# Copy application source
COPY server.js ./
COPY public/ ./public/

# Create logs directory for SQLite database
RUN mkdir -p /app/logs

# The app writes its SQLite database to /app/logs/v4call-ledger.db
# Mount this as a volume to persist data across container restarts:
#   docker run -v /your/host/logs:/app/logs ...
VOLUME ["/app/logs"]

# Expose the internal port (set in .env as PORT, default 3000)
# This does NOT publish the port — docker-compose handles that
EXPOSE 3000

# Health check — verifies the server is responding
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
  CMD wget -qO- http://localhost:${PORT:-3000}/debug-state || exit 1

# Run as non-root user for security
USER node

# Start the server
CMD ["node", "server.js"]
