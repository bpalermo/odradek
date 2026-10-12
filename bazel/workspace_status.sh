#!/usr/bin/env bash
# Supplies the build with the git state that ends up in image push tags.
#
# Every key here is STABLE_. Unprefixed keys go to volatile-status.txt, which
# Bazel treats as constant metadata: an action that embeds one is not
# invalidated when it changes, so a cached action could keep publishing a stale
# value. A commit identity must invalidate when the commit changes. Only the
# image_push target consumes it (see //sidecar), so nothing else is re-run.
# Never fails, so it cannot break a build.
set -euo pipefail

echo "STABLE_GIT_COMMIT $(git rev-parse HEAD 2>/dev/null || echo unknown)"
echo "STABLE_GIT_VERSION $(git describe --tags --always --dirty 2>/dev/null || echo unknown)"
