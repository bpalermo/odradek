"""Linter aspects, enabled by `--config=lint` (see .bazelrc)."""

load("@aspect_rules_lint//lint:buf.bzl", "lint_buf_aspect")
load("@aspect_rules_lint//lint:lint_test.bzl", "lint_test")
load("@aspect_rules_lint//lint:pmd.bzl", "lint_pmd_aspect")
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

# Java: visits java_library and java_binary, so tests (java_test) are not linted.
# Generated sources are skipped. Error Prone and -Xlint run in the compiler; the
# ruleset keeps to what they do not report.
pmd = lint_pmd_aspect(
    binary = Label("//bazel/lint:pmd"),
    rulesets = [Label("//:pmd.xml")],
)
