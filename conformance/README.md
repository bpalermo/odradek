# Conformance scenarios

The client protocol core (see [ADR 1](../docs/adr/0001-shared-client-protocol-core.md)) takes events and returns actions, with no I/O. These scenarios define what it must return. Every implementation of the core, in any language, runs all of them.

## Format

Each file under `scenarios/` is one `odradek.client.v1.Scenario`, written as YAML in the protobuf JSON mapping. The schema, with a description of every event and action, is [`api/odradek/client/v1/core.proto`](../api/odradek/client/v1/core.proto).

A scenario creates one core from its `setup`, then applies its `steps` in order. After each `event`, the core must return exactly the actions under `expect`, in that order. A step without `expect` must return nothing.

- Field names are lowerCamelCase.
- `bytes` fields (keys, values, header values) are base64: `dg==` is `v`.
- The file name is the scenario's `name` plus `.yaml`.

## Running

| Implementation | Command |
|---|---|
| Java core (`//client/core`) | `bazel test //client/core:conformance_test` |
| Python core (`//client/python`, `odradek.core`) | `bazel test //client/python:conformance_test` |

## Changing the protocol

A change to client behaviour starts here: add or edit a scenario, watch it fail, then change each core until it passes. A scenario that only one implementation can pass is a bug in the scenario or in the others.
