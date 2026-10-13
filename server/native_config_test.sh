#!/usr/bin/env bash
# What a native image must do with a configuration, which needs no Kafka broker:
# start and stop cleanly, accept the codecs Kafka ships, and reject, with a clear
# message and exit status 2, a configuration that names a class the image does
# not contain or that violates the schema. Usage: native_config_test.sh <server>
set -euo pipefail

server="$(realpath "$1")"
extension_jar="$(realpath "${EXTENSION_JAR}")"
clock_shim="$(realpath "${CLOCK_SHIM}")"
# Short: a Unix socket path is limited to about 100 bytes, which TEST_TMPDIR exceeds.
dir="$(mktemp -d /tmp/odradek-native.XXXXXX)"
trap 'rm -rf "${dir}"' EXIT
cd "${dir}"
failures=0

fail() {
  echo "FAIL: $*" >&2
  failures=$((failures + 1))
}

# config <name> <yaml>: writes <name>.yaml with a listener in the test directory.
config() {
  cat >"$1.yaml"
  # A free admin port: another test's server may run on this machine at the same time.
  printf 'listener:\n  socketPath: %s/%s.sock\n' "${dir}" "$1" >>"$1.yaml"
  grep -q '^admin:' "$1.yaml" || printf 'admin:\n  port: 0\n' >>"$1.yaml"
}

# rejected <name> <message>: the server exits 2 and says <message>.
rejected() {
  local status=0
  timeout 30 "${server}" --config "$1.yaml" >"$1.log" 2>&1 || status=$?
  if [ "${status}" -ne 2 ]; then
    fail "$1: expected exit status 2, got ${status}"
    cat "$1.log" >&2
  elif ! grep -q -- "$2" "$1.log"; then
    fail "$1: expected '$2' in the log"
    cat "$1.log" >&2
  fi
}

# runs <name> <started> <stopped>: the server logs <started>, and SIGTERM stops it (the JVM exits
# 143, 128 + SIGTERM, after its shutdown hook, and so does the image) with <stopped> in the log.
runs() {
  "${server}" --config "$1.yaml" >"$1.log" 2>&1 &
  local pid=$!
  local listening=0
  for _ in $(seq 1 100); do
    if grep -q -- "$2" "$1.log"; then
      listening=1
      break
    fi
    kill -0 "${pid}" 2>/dev/null || break
    sleep 0.1
  done
  if [ "${listening}" -ne 1 ]; then
    fail "$1: never logged '$2'"
    cat "$1.log" >&2
    kill "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
    return
  fi
  kill -TERM "${pid}"
  local status=0
  wait "${pid}" || status=$?
  if [ "${status}" -ne 0 ] && [ "${status}" -ne 143 ]; then
    fail "$1: SIGTERM ended it with status ${status}"
    cat "$1.log" >&2
  elif ! grep -q -- "$3" "$1.log"; then
    fail "$1: did not log '$3'"
    cat "$1.log" >&2
  fi
}

# serves <name>: the server listens on the app's socket, and SIGTERM stops it.
serves() {
  runs "$1" "^.* -- listening on " " -- stopped"
}

config minimal <<'EOF'
clusters:
  main:
    properties:
      bootstrap.servers: localhost:9092
EOF
serves minimal

# The serializers Kafka ships are looked up by name.
config kafka-codecs <<'EOF'
clusters:
  main:
    properties:
      bootstrap.servers: localhost:9092
codecs:
  text:
    key:
      serializer: org.apache.kafka.common.serialization.StringSerializer
      deserializer: org.apache.kafka.common.serialization.StringDeserializer
    value:
      serializer: org.apache.kafka.common.serialization.ByteArraySerializer
      deserializer: org.apache.kafka.common.serialization.ByteArrayDeserializer
default_codec: text
producers:
  orders:
    clusters: [{cluster: main}]
consumers:
  orders:
    cluster: main
    group_id: orders
EOF
serves kafka-codecs

# A class the image does not contain is rejected at load, like any class that cannot be loaded.
config absent-serializer <<'EOF'
clusters:
  main:
    properties:
      bootstrap.servers: localhost:9092
codecs:
  mine:
    value:
      serializer: com.example.NotInThisImage
      deserializer: org.apache.kafka.common.serialization.ByteArrayDeserializer
producers:
  orders:
    clusters: [{cluster: main}]
    codec: mine
EOF
rejected absent-serializer "class com.example.NotInThisImage cannot be loaded"

config absent-transform <<'EOF'
clusters:
  main:
    properties:
      bootstrap.servers: localhost:9092
codecs:
  mine:
    value:
      transforms:
        - class: com.example.NotInThisImage
producers:
  orders:
    clusters: [{cluster: main}]
    codec: mine
EOF
rejected absent-transform "class com.example.NotInThisImage cannot be loaded"

# A class that is in the image but is not a codec.
config wrong-type <<'EOF'
clusters:
  main:
    properties:
      bootstrap.servers: localhost:9092
codecs:
  mine:
    value:
      serializer: org.apache.kafka.common.serialization.StringDeserializer
      deserializer: org.apache.kafka.common.serialization.StringDeserializer
producers:
  orders:
    clusters: [{cluster: main}]
    codec: mine
EOF
rejected wrong-type "does not implement org.apache.kafka.common.serialization.Serializer"

# An extension directory holds jars the image cannot load classes from.
mkdir -p extensions
cp "${extension_jar}" extensions/
config extension-jar <<EOF
extensions_dir: ${dir}/extensions
clusters:
  main:
    properties:
      bootstrap.servers: localhost:9092
codecs:
  mine:
    value:
      serializer: io.github.bpalermo.odradek.server.nativefixture.FixtureSerializer
      deserializer: org.apache.kafka.common.serialization.ByteArrayDeserializer
producers:
  orders:
    clusters: [{cluster: main}]
    codec: mine
EOF
rejected extension-jar \
  "class io.github.bpalermo.odradek.server.nativefixture.FixtureSerializer cannot be loaded"

# The schema is enforced: protovalidate rules and CEL expressions run in the image.
config violations <<'EOF'
admin:
  port: 70000
clusters:
  main:
    properties:
      bootstrap.servers: localhost:9092
    producer_properties:
      acks: "1"
producers:
  orders:
    clusters: [{cluster: nowhere}]
EOF
rejected violations "admin.port: must be less than or equal to 65535"
grep -q "every producer must reference entries in clusters" violations.log ||
  fail "violations: the CEL rule on producers was not evaluated"
grep -q "acks must be all" violations.log || fail "violations: the CEL rule on acks was not evaluated"

# A dispatcher (ADR 6) with neither producers nor consumers and no TLS: it serves the delivery
# port, says that the port is open to whoever reaches it, and does not open the socket of an app
# that is not there. (With TLS it is the end-to-end suite's: that needs certificates.)
config dispatch <<'EOF'
clusters:
  main:
    properties:
      bootstrap.servers: localhost:9092
dispatch:
  port: 0
EOF
runs dispatch "delivery service on port [0-9]*, advertised as .*, PLAINTEXT" \
  "delivery service stopped"
grep -q "THE DELIVERY PORT IS PLAINTEXT AND UNAUTHENTICATED" dispatch.log ||
  fail "dispatch: no warning about the plaintext port"
[ ! -e dispatch.sock ] || fail "dispatch: opened the app's socket"

# A policy on a port without TLS would be a policy nobody can match, or one that is ignored:
# such a dispatcher does not start.
config dispatch-policy-plaintext <<'EOF'
clusters:
  main:
    properties:
      bootstrap.servers: localhost:9092
dispatch:
  port: 0
  policy:
    - trustDomain: example.org
      cluster: main
      topics: ["orders.*"]
      groups: [billing]
EOF
rejected dispatch-policy-plaintext "a policy needs tls"

# Credentials that are not there are a failure to start, not a plaintext port.
config dispatch-tls-missing <<'EOF'
clusters:
  main:
    properties:
      bootstrap.servers: localhost:9092
dispatch:
  port: 0
  tls:
    files:
      dir: /nonexistent/odradek-delivery
EOF
status=0
timeout 30 "${server}" --config dispatch-tls-missing.yaml >dispatch-tls-missing.log 2>&1 || status=$?
[ "${status}" -eq 1 ] || fail "dispatch-tls-missing: expected exit status 1, got ${status}"
grep -q "TLS credentials unavailable: dispatch.tls: " dispatch-tls-missing.log ||
  fail "dispatch-tls-missing: the log does not name dispatch.tls"
if grep -q "delivery service on port" dispatch-tls-missing.log; then
  fail "dispatch-tls-missing: it listened all the same"
fi

# A server that receives what it consumes from a dispatcher (ADR 6) has no cluster of its own,
# and starts, and is ready, with no dispatcher to be found: it connects when an app subscribes.
# Without TLS it says that the connection is not secured.
config dispatched-consumer <<'EOF'
dispatcher:
  address: odradek-dispatcher.kafka.svc:9465
consumers:
  orders:
    source: CONSUMER_SOURCE_DISPATCHER
    cluster: main
    groupId: billing
    ordering: ORDERING_PARTITION
EOF
serves dispatched-consumer
grep -q "CONNECTION TO THE DISPATCHER AT odradek-dispatcher.kafka.svc:9465 IS PLAINTEXT" \
  dispatched-consumer.log || fail "dispatched-consumer: no warning about the connection"

# A dispatcher is recognized by its SPIFFE ID or not at all: there is no host name to fall back on.
config dispatcher-tls-anyone <<'EOF'
dispatcher:
  address: odradek-dispatcher.kafka.svc:9465
  tls:
    files:
      dir: /etc/odradek/delivery
consumers:
  orders:
    source: CONSUMER_SOURCE_DISPATCHER
    cluster: main
    groupId: billing
EOF
rejected dispatcher-tls-anyone "tls needs accepted_spiffe_ids or accepted_trust_domain"

# Its rules run in the image like the others.
config dispatch-violations <<'EOF'
clusters:
  main:
    properties:
      bootstrap.servers: localhost:9092
dispatch:
  advertisedAddress: not-an-address
  policy:
    - cluster: nowhere
      topics: ["orders.*.v1"]
      groups: [billing]
consumers:
  orders:
    source: CONSUMER_SOURCE_DISPATCHER
    cluster: main
    groupId: billing
    onExhausted: ON_EXHAUSTED_DEAD_LETTER
EOF
rejected dispatch-violations "every rule of dispatch.policy must reference an entry in clusters"
for message in \
  "a rule needs spiffe_ids or trust_domain" \
  "a policy needs tls" \
  "dispatcher must be set when a consumer's source is CONSUMER_SOURCE_DISPATCHER" \
  "dead_letter_topic must be set when on_exhausted is ON_EXHAUSTED_DEAD_LETTER" \
  "dispatch.advertised_address: " \
  'dispatch.policy\[0\].topics\[0\]: '; do
  grep -q -- "${message}" dispatch-violations.log ||
    fail "dispatch-violations: expected '${message}' in the log"
done

printf 'clusters: [unterminated\n' >yaml-error.yaml
rejected yaml-error "not valid YAML"

# A machine that booted after the one that built the image: its monotonic clock reads less
# than it did during the build. A clock captured at build time (Netty's was) then runs
# negative, every timer on a Netty event loop fires at once, and every gRPC call made on one
# dies of its deadline the moment it starts. Seen here through the metrics export: sent to a
# gRPC server that is not a collector, it must be answered "unimplemented", not time out.
config not-a-collector <<'EOF'
clusters:
  main:
    properties:
      bootstrap.servers: localhost:9092
EOF
"${server}" --config not-a-collector.yaml >not-a-collector.log 2>&1 &
collector_pid=$!
port=""
for _ in $(seq 1 100); do
  port="$(sed -nE 's/.*health service on port ([0-9]+).*/\1/p' not-a-collector.log | head -1)"
  [ -n "${port}" ] && break
  sleep 0.1
done
if [ -z "${port}" ]; then
  fail "young-machine: the stand-in server did not report its port"
  cat not-a-collector.log >&2
else
  gcc -shared -fPIC -o monotonic_back.so "${clock_shim}" -ldl
  config young-machine <<EOF
telemetry:
  endpoint: http://localhost:${port}
  exportInterval: 1s
clusters:
  main:
    properties:
      bootstrap.servers: localhost:9092
EOF
  LD_PRELOAD="${dir}/monotonic_back.so" "${server}" --config young-machine.yaml \
    >young-machine.log 2>&1 &
  young_pid=$!
  exported=0
  for _ in $(seq 1 100); do
    if grep -q "Failed to export" young-machine.log; then
      exported=1
      break
    fi
    sleep 0.1
  done
  # A few more exports, so that one timing out is seen if any does.
  sleep 3
  kill -TERM "${young_pid}" 2>/dev/null || true
  wait "${young_pid}" 2>/dev/null || true
  if [ "${exported}" -ne 1 ]; then
    fail "young-machine: no export was attempted"
    cat young-machine.log >&2
  elif grep -qi "deadline exceeded" young-machine.log; then
    fail "young-machine: a gRPC call timed out at once; a clock was captured when the image was built"
    grep -i "deadline exceeded" young-machine.log | head -3 >&2
  fi
fi
kill -TERM "${collector_pid}" 2>/dev/null || true
wait "${collector_pid}" 2>/dev/null || true

if [ "${failures}" -ne 0 ]; then
  echo "${failures} check(s) failed" >&2
  exit 1
fi
echo "all checks passed"
