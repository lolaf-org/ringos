# ringos-lib-impl-all

This artifact carries no code of its own. It is the aggregator you depend on to get a working
`RingBuffer` implementation without choosing one: its dependencies bundle the Unsafe, Java 11 Unsafe
and MethodHandle builders, and `RingBufferFactory` selects between them at runtime.

Maven Central requires a `-sources.jar` and a `-javadoc.jar` beside every non-`pom` artifact, so these
two are placeholders holding this file. The sources and the documentation are in the artifacts it
bundles:

- `org.lolaf.ringos:ringos-lib-api` — the API you compile against: `RingBuffer`,
  `RingBufferFactory`, the builder SPI, the idle strategies
- `org.lolaf.ringos:ringos-lib-impl-unsafe`, `…-lib-impl-unsafe-11`, `…-lib-impl-method-handle` —
  the implementations, selected at runtime and not meant to be referenced directly
- `org.lolaf.ringos:ringos-unsafe-operations-impl-all` — the matching low-level memory access

Sources: https://github.com/lolaf-org/ringos
