#!/usr/bin/env bash
# Regenerates api/baseline.binpb, the image that `//api:breaking_test` compares
# the current protos against.
set -euo pipefail
dest="${BUILD_WORKSPACE_DIRECTORY:?run with: bazel run //api:update_baseline}/api/baseline.binpb"
cp -f "${IMAGE:?}" "$dest"
chmod 0644 "$dest"
echo "wrote ${dest}"
