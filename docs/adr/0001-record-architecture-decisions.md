# 1. Record architecture decisions

## Status

Accepted

## Context

Odradek's design document describes the system as a whole and is rewritten in place when the design changes. That keeps it coherent, but it loses the history: a reader cannot tell which parts were decided deliberately, what the alternatives were, or whether the reasons still hold.

Decisions are also being made before they belong in the design document, such as how client libraries in other languages will be built. They need a home that does not require the design to be rewritten first.

## Decision

We will keep architecture decision records in `docs/adr`, in the format described by Michael Nygard in [Documenting Architecture Decisions](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions).

- Each record is one Markdown file named `NNNN-short-title.md`, numbered sequentially. Numbers are never reused.
- Each record has the sections Title, Status, Context, Decision and Consequences, is written in full sentences, and stays within a page or two.
- Status is Proposed, Accepted, Deprecated or Superseded. A superseded record stays in place and links to the record that replaces it.
- An accepted record is not edited to change its decision. A changed decision is a new record.
- A record covers one decision that affects structure, dependencies, interfaces or guarantees. Details that are cheap to change do not need one.
- `docs/adr/README.md` lists every record with its status.

## Consequences

The reasons behind the design are kept even when the design document is rewritten. Someone who disagrees with a decision can see what it rested on and propose a record that supersedes it.

Records and the design document can drift apart. When a record is accepted, the design document is updated in the same change if the decision alters what it describes.

Writing a record is a small cost on every significant decision. Decisions made before this record exist only in the design document and the commit history; they will get records only if they are revisited.
