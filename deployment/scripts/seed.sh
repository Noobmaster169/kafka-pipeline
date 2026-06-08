#!/usr/bin/env bash
# Seed AWAS reference data (lanes, cameras, cars) into MongoDB.
# Usage: deployment/scripts/seed.sh [--reset] [--cars-limit N]
set -euo pipefail

# Resolve the module root (two levels up from this script) and run the seeder from src/.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT/src"
exec python seed_db.py "$@"
