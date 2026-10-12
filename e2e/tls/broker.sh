#!/usr/bin/env bash
# Starts the third broker of the end-to-end suite: one that only talks mutual
# TLS on its client listener, with certificates made for the occasion.
#
#   e2e/tls/broker.sh <dir>
#
# <dir> receives the certificates: ca/, broker/, client/ and client-next/. It must
# not be under /tmp, which Bazel's sandbox hides from the tests. The
# broker listens on localhost:9094 (TLS, client certificate required) and on
# localhost:9095 (plaintext, for the tests' own topic administration). Then:
#
#   export ODRADEK_E2E_KAFKA_TLS=localhost:9094
#   export ODRADEK_E2E_KAFKA_TLS_ADMIN=localhost:9095
#   export ODRADEK_E2E_TLS_DIR=<dir>
#
# Needs docker and bazel. Stop the broker with `docker rm -f odradek-kafka-tls`.
set -euo pipefail

dir="$(mkdir -p "$1" && cd "$1" && pwd)"
image="apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837"
name="odradek-kafka-tls"

cd "$(dirname "$0")/../.."
# ODRADEK_BAZEL_FLAGS: extra flags, e.g. --config=ci, so CI keeps one configuration.
# shellcheck disable=SC2086
bazel run ${ODRADEK_BAZEL_FLAGS:-} //e2e:pki -- "${dir}"

# Kafka's PEM key store is one file: the private key, then the certificate chain.
cat "${dir}/broker/tls.key" "${dir}/broker/tls.crt" >"${dir}/broker/keystore.pem"
# The broker runs as another user.
chmod -R a+rX "${dir}"

# The listener is named MTLS, not SSL: for a listener called SSL the image's
# start script insists on JKS key stores and password files.
docker rm -f "${name}" >/dev/null 2>&1 || true
docker run -d --name "${name}" \
  -p 9094:9094 -p 9095:9095 \
  -v "${dir}/broker:/certs:ro" \
  -e KAFKA_NODE_ID=3 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=MTLS://:9094,PLAINTEXT://:9095,CONTROLLER://:19094 \
  -e KAFKA_ADVERTISED_LISTENERS=MTLS://localhost:9094,PLAINTEXT://localhost:9095 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,MTLS:SSL \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=3@localhost:19094 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  -e KAFKA_SSL_KEYSTORE_TYPE=PEM \
  -e KAFKA_SSL_KEYSTORE_LOCATION=/certs/keystore.pem \
  -e KAFKA_SSL_TRUSTSTORE_TYPE=PEM \
  -e KAFKA_SSL_TRUSTSTORE_LOCATION=/certs/ca.crt \
  -e KAFKA_SSL_CLIENT_AUTH=required \
  "${image}" >/dev/null

for _ in $(seq 60); do
  if docker exec "${name}" /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server localhost:9095 >/dev/null 2>&1; then
    echo "broker ${name} is up: TLS on localhost:9094, plaintext on localhost:9095"
    exit 0
  fi
  if [ "$(docker inspect -f '{{.State.Running}}' "${name}")" != "true" ]; then
    docker logs "${name}" >&2
    echo "broker ${name} exited" >&2
    exit 1
  fi
  sleep 2
done
docker logs "${name}" >&2
echo "broker ${name} did not come up" >&2
exit 1
