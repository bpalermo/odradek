<!--
Keep it short. The interesting part is usually "how it was verified" and
"what could go wrong", not a restatement of the diff.
-->

## What

<!-- One or two sentences. What changes for an app using the sidecar, or for someone operating it? -->

## Why

<!-- The problem this solves. If it fixes a bug, what did the bug look like? -->

## How it was verified

<!-- Say what you actually ran, and what the result was. -->

- [ ] `bazel test //...`
- [ ] `bazel run //:format.check`
- [ ] `bazel test //e2e/... --config=e2e` against two KRaft brokers, if the change touches produce, consume or configuration behaviour
- [ ] API or config proto changed: the baseline was regenerated on purpose, and `docs/design.md` matches
