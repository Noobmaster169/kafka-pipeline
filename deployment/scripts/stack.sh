#!/usr/bin/env bash
# Single entry point for the whole AWAS A3 stack. Wraps docker compose (Zookeeper, Kafka,
# Mongo, Spark all on kafka-net) plus the in-container Spark actions, so you start, stop,
# and drive everything from one command — no manual `docker run` / `docker rm` sequences.
#
#   deployment/scripts/stack.sh up        # build + start all four containers (detached)
#   deployment/scripts/stack.sh down      # stop + remove all containers and the network
#   deployment/scripts/stack.sh restart   # down, then up
#   deployment/scripts/stack.sh ps        # show container status
#   deployment/scripts/stack.sh logs [svc]# tail logs (all, or one service e.g. kafka)
#   deployment/scripts/stack.sh seed [...] # seed Mongo reference data (passes flags through)
#   deployment/scripts/stack.sh verify    # offline join test inside the Spark container
#   deployment/scripts/stack.sh pipeline  # run the streaming pipeline (--reset)
#   deployment/scripts/stack.sh sim [...]  # run the traffic simulator INSIDE the stack (resolves kafka/mongo)
#   deployment/scripts/stack.sh shell     # interactive shell in /app/src (Spark container)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE=(docker compose -f "$ROOT/deployment/config/docker-compose.yml")
CMD="${1:-up}"
shift || true

case "$CMD" in
  up)
    # Clear any leftover containers with these names (from a previous run or hand-started),
    # so a fresh start never hits a "name already in use" collision.
    docker rm -f zookeeper kafka mongo spark >/dev/null 2>&1 || true
    # Ensure the shared external network exists before Compose attaches to it.
    docker network create kafka-net >/dev/null 2>&1 || true
    "${COMPOSE[@]}" up -d --build
    ;;
  down)     "${COMPOSE[@]}" down ;;
  restart)  "${COMPOSE[@]}" down; "${COMPOSE[@]}" up -d --build ;;
  ps)       "${COMPOSE[@]}" ps ;;
  logs)     "${COMPOSE[@]}" logs -f "$@" ;;
  seed)     "$ROOT/deployment/scripts/seed.sh" "$@" ;;
  verify)   docker exec -it spark bash -lc "cd /app/src && python -m pipeline.verify_detect" ;;
  pipeline) docker exec -it spark bash -lc "cd /app/src && python -m pipeline.run --reset 2>&1 | grep --line-buffered -v StreamingJoinHelper" ;;
  sim)      docker exec -it spark bash -lc "cd /app/src && python -m simulator.run $*" ;;
  shell)    docker exec -it spark bash -lc "cd /app/src && exec bash" ;;
  *)
    echo "usage: stack.sh {up|down|restart|ps|logs|seed|verify|pipeline|sim|shell}" >&2
    exit 1
    ;;
esac
