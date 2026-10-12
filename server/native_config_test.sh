#!/usr/bin/env bash
# What a native image must do with a configuration, which needs no Kafka broker:
# start and stop cleanly, accept the codecs Kafka ships, and reject, with a clear
# message and exit status 2, a configuration that names a class the image does
# not contain or that violates the schema. Usage: native_config_test.sh <server>
set -euo pipefail

server="$(realpath "$1")"
extension_jar="$(realpath "${EXTENSION_JAR}")"
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

# serves <name>: the server listens, and SIGTERM stops it (the JVM exits 143, 128 + SIGTERM,
# after its shutdown hook, and so does the image).
serves() {
  "${server}" --config "$1.yaml" >"$1.log" 2>&1 &
  local pid=$!
  local listening=0
  for _ in $(seq 1 100); do
    if grep -q "listening on" "$1.log"; then
      listening=1
      break
    fi
    kill -0 "${pid}" 2>/dev/null || break
    sleep 0.1
  done
  if [ "${listening}" -ne 1 ]; then
    fail "$1: never listened"
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
  elif ! grep -q "stopped" "$1.log"; then
    fail "$1: did not log that it stopped"
    cat "$1.log" >&2
  fi
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

printf 'clusters: [unterminated\n' >yaml-error.yaml
rejected yaml-error "not valid YAML"

if [ "${failures}" -ne 0 ]; then
  echo "${failures} check(s) failed" >&2
  exit 1
fi
echo "all checks passed"
