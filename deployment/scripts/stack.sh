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
#   deployment/scripts/stack.sh joincmp [...] # offline adjacent vs all-pairs join-cost experiment
#   deployment/scripts/stack.sh pipeline  # run the streaming pipeline (--reset); honours JOIN_STRATEGY
#   deployment/scripts/stack.sh topics    # create camera-events with KAFKA_TOPIC_PARTITIONS + describe
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
  joincmp)  docker exec -it spark bash -lc "cd /app && python -m benchmarks.join_compare $*" ;;
  topics)
    # The unit's Kafka image (0.10.x) predates the CreateTopics admin API, so the Python
    # client can't create topics — we use the broker's own kafka-topics.sh (via ZooKeeper).
    # Idempotent (--if-not-exists) and runs only in the kafka container, never in a hot path.
    docker exec \
      -e TOPIC="${KAFKA_TOPIC:-camera-events}" \
      -e PARTS="${KAFKA_TOPIC_PARTITIONS:-6}" \
      kafka bash -lc '
        KT="$(command -v kafka-topics.sh || true)"
        for cand in /opt/kafka/bin/kafka-topics.sh /usr/bin/kafka-topics.sh /kafka/bin/kafka-topics.sh; do
          [ -z "$KT" ] && [ -x "$cand" ] && KT="$cand"
        done
        [ -z "$KT" ] && { echo "kafka-topics.sh not found in kafka container" >&2; exit 1; }
        # The broker registers in ZooKeeper a few seconds AFTER `stack.sh up` returns; creating
        # a topic before that fails with "Replication factor: 1 larger than available brokers: 0".
        # Retry until the broker is registered (up to ~60s) instead of failing on the race.
        for attempt in 1 2 3 4 5 6 7 8 9 10 11 12; do
          if "$KT" --zookeeper zookeeper:2181 --create --topic "$TOPIC" \
                --partitions "$PARTS" --replication-factor 1 --if-not-exists; then
            break
          fi
          echo "[topics] broker not registered in ZooKeeper yet (attempt $attempt/12) — retrying in 5s ..." >&2
          sleep 5
        done
        "$KT" --zookeeper zookeeper:2181 --describe --topic "$TOPIC"
      '
    ;;
  # JOIN_STRATEGY passes through so the §8 strategy sweep is just:
  #   JOIN_STRATEGY=all-pairs stack.sh pipeline   vs   JOIN_STRATEGY=adjacent stack.sh pipeline
  pipeline) docker exec -e JOIN_STRATEGY="${JOIN_STRATEGY:-adjacent}" -it spark bash -lc "cd /app/src && python -m pipeline.run --reset 2>&1 | grep --line-buffered -v StreamingJoinHelper" ;;
  sim)      docker exec -it spark bash -lc "cd /app/src && python -m simulator.run $*" ;;
  shell)    docker exec -it spark bash -lc "cd /app/src && exec bash" ;;
  *)
    echo "usage: stack.sh {up|down|restart|ps|logs|seed|verify|joincmp|pipeline|topics|sim|shell}" >&2
    exit 1
    ;;
esac
