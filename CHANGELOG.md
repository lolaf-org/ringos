# Changelog

All notable changes to this project are recorded here, in the format of
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). This project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Each released version needs its own `## [x.y.z] - YYYY-MM-DD` heading here **before** the release is
cut: the release workflow refuses to run without one, and the GitHub Release for the tag is created
with that section as its body. Write it in the commit that precedes the release, together with the
matching `[x.y.z]:` link definition at the foot of the file.

There is deliberately no `[Unreleased]` section, which is where Keep a Changelog would collect notes
between releases. A section is written when the version it belongs to is being cut, so its heading
carries the right number and date the first time and the workflow's check has exactly one heading it
could mean.

## [1.0.0] - 2026-08-27

First public release.

### Added

- **Ring buffers** (`ringos-lib-api`, with `ringos-lib-impl-all` at runtime) — pre-allocated MPMC,
  MPSC, SPMC and SPSC ring buffers whose slots are mutated in place rather than replaced, so
  publishing an event allocates nothing. Three implementations are shipped and one is chosen at
  runtime: `sun.misc.Unsafe` for JDK 12+, a JDK 11 variant, and a `VarHandle` fallback.
- **Idle strategies** — busy-spin, backoff, yield and wait-notify, spanning the CPU-burn against
  wake-up-latency trade-off, plus `FastThreadLocal`.
- **Hashed wheel timer** (`ringos-timer`) — built on the ringos MPSC buffer, for the
  many-short-lived-timeouts shape that network code has.
- **Linux syscall bindings** (`ringos-clib`) — timer slack and thread scheduling policy, via JNA.
- **Unsafe operations façade** (`ringos-unsafe-operations-api`, with
  `ringos-unsafe-operations-impl-all` at runtime) — one API over the JDK-internal memory operations,
  implemented three times because those internals change across JDK 11 through 25.
- **Ring buffer contract test kit** (`ringos-lib-impl-testkit`) — the shared contract every
  implementation is held to. Published as a test-jar for anyone writing their own implementation;
  not part of the release bundle.

### Notes

- Java 11 or later, compiled to Java 11 bytecode, tested on JDK 11 through 25.
- No required dependencies beyond SLF4J, and JNA for `ringos-clib`.
- All artifacts are signed, carry sources and javadoc, and are built reproducibly — the jars from a
  given tag are byte-identical to the published ones.

[1.0.0]: https://github.com/lolaf-org/ringos/releases/tag/v1.0.0
