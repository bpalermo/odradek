.PHONY: build test format format-check lint repin tidy

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
