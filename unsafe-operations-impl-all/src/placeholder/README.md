# ringos-unsafe-operations-impl-all

This artifact carries no code of its own. It is the aggregator you depend on to get an
`UnsafeOperations` implementation without choosing one: its dependencies bundle every JDK-range
implementation, and the matching one is loaded reflectively on the JDK that is running.

Maven Central requires a `-sources.jar` and a `-javadoc.jar` beside every non-`pom` artifact, so these
two are placeholders holding this file. The sources and the documentation are in the artifacts it
bundles:

- `org.lolaf.ringos:ringos-unsafe-operations-api` — the SPI you compile against
- `org.lolaf.ringos:ringos-unsafe-operations-impl-11-14`, `…-impl-15-24`, `…-impl-25-provider` —
  the JDK-range implementations, loaded reflectively and not meant to be referenced directly

Sources: https://github.com/lolaf-org/ringos
