.PHONY: build test format format-check lint repin tidy image image-load native baseline e2e

build:
	@bazel build //...

test:
	@bazel test //...

format:
	@bazel run //:format

format-check:
	@bazel run //:format.check

lint:
	@bazel build --config=lint //...

# Re-resolve and re-pin the Maven tree after editing MODULE.bazel's maven.install.
repin:
	@REPIN=1 bazel run @maven//:pin

tidy:
	@bazel mod tidy

image:
	@bazel build //server:image_index

image-load:
	@bazel run //server:image_load

# The GraalVM native image of the server; excluded from `make build`.
native:
	@bazel build //server:server_native

# Regenerate api/baseline.binpb from the current protos (a release, or an accepted break).
baseline:
	@bazel run //api:update_baseline

# Needs two Kafka brokers; see README. Broker addresses default to localhost.
e2e:
	@ODRADEK_E2E_KAFKA_A=$${ODRADEK_E2E_KAFKA_A:-localhost:9092} \
	  ODRADEK_E2E_KAFKA_B=$${ODRADEK_E2E_KAFKA_B:-localhost:9093} \
	  bazel test //e2e/... --config=e2e
