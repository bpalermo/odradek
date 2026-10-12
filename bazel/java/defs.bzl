"""Shared Java settings for first-party targets."""

# Every javac and Error Prone warning is an error in first-party code. Not a
# global --javacopt: external code (gRPC is built from source) must not be held to it.
JAVACOPTS = [
    "-Xlint:all",
    "-Werror",
]
