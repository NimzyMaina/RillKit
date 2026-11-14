#!/usr/bin/env bash
# Start a throwaway Redis or Valkey Cluster in one container, for the integration tests.
#
#   scripts/test-cluster.sh redis  [first-port]   # redis:7 image, redis-server
#   scripts/test-cluster.sh valkey [first-port]   # valkey/valkey:8 image, valkey-server
#   scripts/test-cluster.sh stop   redis|valkey
#
# Six nodes (three primaries, three replicas) run on consecutive ports starting at first-port
# (default 7101, because macOS AirPlay holds 7000). Every node announces 127.0.0.1 and its own
# port, and the ports are published one to one, so a client on the host reaches each node at the
# address the cluster reports.
set -euo pipefail

backend="${1:?usage: test-cluster.sh redis|valkey|stop [first-port]}"

if [ "$backend" = "stop" ]; then
  docker rm -f "rillkit-${2:?which cluster}-cluster" >/dev/null
  exit 0
fi

first="${2:-7101}"
last=$((first + 5))
case "$backend" in
  redis)  image="${CLUSTER_IMAGE:-redis:7}";         server=redis-server;  cli=redis-cli ;;
  valkey) image="${CLUSTER_IMAGE:-valkey/valkey:8}"; server=valkey-server; cli=valkey-cli ;;
  *) echo "unknown backend: $backend" >&2; exit 2 ;;
esac
name="rillkit-${backend}-cluster"

docker rm -f "$name" >/dev/null 2>&1 || true
docker run -d --name "$name" -p "${first}-${last}:${first}-${last}" "$image" sh -c "
  for port in \$(seq $first $last); do
    mkdir -p /data/\$port
    $server --port \$port --dir /data/\$port --cluster-enabled yes \
      --cluster-config-file nodes.conf --cluster-node-timeout 2000 \
      --cluster-announce-ip 127.0.0.1 --appendonly no --save '' --daemonize yes
  done
  sleep infinity
" >/dev/null

for port in $(seq "$first" "$last"); do
  until docker exec "$name" "$cli" -p "$port" ping >/dev/null 2>&1; do sleep 0.2; done
done

nodes=""
for port in $(seq "$first" "$last"); do nodes="$nodes 127.0.0.1:$port"; done
# shellcheck disable=SC2086
docker exec "$name" "$cli" --cluster create $nodes --cluster-replicas 1 --cluster-yes >/dev/null

until docker exec "$name" "$cli" -p "$first" cluster info | grep -q 'cluster_state:ok'; do
  sleep 0.2
done
echo "$backend cluster ready on 127.0.0.1:${first}-${last}"
