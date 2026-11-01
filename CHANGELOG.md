# Changelog

All notable changes to this project are recorded here, in the format of
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). This project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Each released version needs its own `## [x.y.z] - YYYY-MM-DD` heading here **before** the release is
cut: the release workflow refuses to run without one, and the GitHub Release for the tag is created
with that section as its body. Write it in the commit that precedes the release, together with the
matching `[x.y.z]:` link definition at the foot of the file.

## [Unreleased]

### Added

- **Byte ring buffers**: variable-length frames of bytes in one off-heap buffer, written and read without
  allocating, from `ByteRingBufferFactory`.
  - `queue(capacity)`: any number of writer threads, one reader. A full queue refuses a write.
  - `broadcast(capacity, producers)`: one writer thread (`ProducerType.SINGLE`) or several (`ProducerType.MULTI`), and
    any number of independent readers. A write never waits: the oldest frames are overwritten, and a reader left
    behind is told it was lapped instead of being handed a damaged frame. Readers can resume from a saved
    position, and can fill a buffer with as many frames as fit: a frame too large for the room left is reported and
    left for the next read. A reader can step back to the frame it last read with `unread()`.
  - Readers that must not be lapped: a reader from `newGatingReader()` holds back every `writeRetrying`, which waits
    for room as long as its `RetryStrategy` allows and then gives up without writing (`GATED`). Such a reader can
    always `unread()`, and stops holding writes back once closed. A plain `write` still never waits.
  - On Java 16 and later the frames are copied as fast as Agrona's equivalents; on Java 11 to 15 large frames are
    several times slower. No JVM flags are needed.
- **`ProducerType`** (`SINGLE`, `MULTI`) says who may write to a buffer: it is what you pass to
  `ByteRingBufferFactory.broadcast`, and `RingBufferFactory.AccessType.getProducerType()` gives the producer half of an
  access type.
- **New runtime artifact** `ringos-lib-impl-method-handle-16-provider`, part of `ringos-lib-impl-all`, so nothing
  changes for applications depending on the aggregator.

## [0.9.1] - 2026-09-26

### Added

- **Offers that can give up**: `RingBuffer.offerRetrying(...)`, one per `offerBlocking` overload,
  waits for room like `offerBlocking` but stops when the `RetryStrategy` you pass says so, and then
  returns `false` without storing anything. Use it when a producer must not wait forever, for
  instance once the consumer has shut down. `RetryStrategy.idlingWhile(idleStrategy, keepTrying)`
  covers the common case: wait on an idle strategy for as long as a condition holds.

## [0.9.0] - 2026-09-09

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

[0.9.1]: https://github.com/lolaf-org/ringos/releases/tag/v0.9.1
[0.9.0]: https://github.com/lolaf-org/ringos/releases/tag/v0.9.0
