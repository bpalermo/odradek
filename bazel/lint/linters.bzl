"""Linter aspects, enabled by `--config=lint` (see .bazelrc)."""

load("@aspect_rules_lint//lint:buf.bzl", "lint_buf_aspect")
load("@aspect_rules_lint//lint:lint_test.bzl", "lint_test")
load("@aspect_rules_lint//lint:ruff.bzl", "lint_ruff_aspect")

buf = lint_buf_aspect(
    config = Label("//:buf.yaml"),
)

buf_test = lint_test(aspect = buf)

# Python: visits py_library, py_binary and py_test. Generated sources are skipped.
ruff = lint_ruff_aspect(
    binary = Label("@aspect_rules_lint//lint:ruff_bin"),
    configs = [Label("//:ruff.toml")],
)
