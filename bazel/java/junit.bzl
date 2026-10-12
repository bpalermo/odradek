"""java_junit_test: run JUnit 6 (Jupiter) tests under `bazel test`.

Bazel's built-in java_test runner speaks JUnit 4. This macro instead runs the
JUnit Platform console launcher, selecting the given test classes explicitly
(classpath scanning is unnecessary and slow under Bazel's runfiles layout).
"""

load("@rules_java//java:defs.bzl", "java_test")

def java_junit_test(name, test_classes, runtime_deps = [], **kwargs):
    """A java_test whose tests are JUnit 6 classes.

    Args:
      name: target name.
      test_classes: fully qualified names of the test classes to run.
      runtime_deps: extra runtime deps; the JUnit Jupiter engine and Platform console are added.
      **kwargs: forwarded to java_test (srcs, deps, size, ...).
    """
    java_test(
        name = name,
        main_class = "org.junit.platform.console.ConsoleLauncher",
        use_testrunner = False,
        args = ["execute", "--fail-if-no-tests", "--disable-banner", "--details=summary"] +
               ["--select-class=" + c for c in test_classes],
        runtime_deps = runtime_deps + [
            "@maven//:org_junit_jupiter_junit_jupiter_engine",
            "@maven//:org_junit_platform_junit_platform_console",
        ],
        **kwargs
    )
