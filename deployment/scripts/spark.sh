#!/usr/bin/env bash
# Backwards-compatible alias. The Spark container is now part of the integrated stack
# (deployment/config/docker-compose.yml), so this just forwards to stack.sh — keeping the
# old `spark.sh up|verify|pipeline|shell` commands working.
#
#   spark.sh up        -> stack.sh up        (builds + starts the whole stack)
#   spark.sh verify    -> stack.sh verify
#   spark.sh pipeline  -> stack.sh pipeline
#   spark.sh shell     -> stack.sh shell
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
exec "$ROOT/deployment/scripts/stack.sh" "$@"
