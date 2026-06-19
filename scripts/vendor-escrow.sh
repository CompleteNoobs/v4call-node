#!/usr/bin/env sh
# ── vendor-escrow.sh ──────────────────────────────────────────────────────────
# Vendor the sibling escrow-core into ./vendor/escrow-core so the Docker build
# can see it. The Docker build context is this folder (v4call-node), so the real
# sibling at ../escrow-core lives OUTSIDE the context and `file:../escrow-core`
# cannot resolve during `docker compose build`. This copies a clean source-only
# snapshot into the context; the Dockerfile then places it at /escrow-core (a
# sibling of /app) so the UNCHANGED package.json `file:../escrow-core` resolves
# in-container too.
#
# Bare-metal deploys do NOT need this — they use the real sibling directly. This
# is a Docker-only pre-build step (see `npm run docker:build`). vendor/ is
# git-ignored on purpose: we never commit a copy of the money library (drift).
set -eu

here="$(cd "$(dirname "$0")/.." && pwd)"
src="$here/../escrow-core"
dst="$here/vendor/escrow-core"

if [ ! -f "$src/package.json" ]; then
  echo "ERROR: escrow-core not found at $src" >&2
  echo "       Clone it as a sibling of v4call-node (same parent dir) and retry." >&2
  exit 1
fi

rm -rf "$here/vendor"
mkdir -p "$here/vendor"
cp -r "$src" "$dst"

# Strip everything that must not enter the image: host-built native modules
# (wrong arch / rebuilt in-container anyway), git history, and any local ledger
# DBs. Keeps the build context small and reproducible.
rm -rf "$dst/node_modules" "$dst/.git"
rm -f "$dst"/*.db "$dst"/*.db-journal "$dst"/*.db-wal "$dst"/*.db-shm 2>/dev/null || true

ver="$(grep -m1 '"version"' "$dst/package.json" | sed 's/.*"version" *: *"\([^"]*\)".*/\1/')"
echo "Vendored escrow-core@${ver:-?} -> vendor/escrow-core (node_modules/.git/db stripped)."
