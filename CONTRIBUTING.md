# Contributing to ringos

Issues and pull requests are welcome. Ringos is a small library with one maintainer, so the most
useful thing a change can be is *narrow*: one topic, tests that fail without it, and a description of
what you measured.

## Building

```bash
./mvnw -T1C clean install
```

**JDK 17 or newer is required to build**, even though the library itself runs on JDK 11 and ships
Java 11 bytecode: the test stack needs 17, and the shared surefire arguments include a flag that JDK
16 and older reject outright. The build enforces this and says so.

**Two toolchains are required as well.** Three modules compile against JDK internals that moved
between releases, so they build on a specific JDK rather than the one running Maven:
`lib-impl-unsafe-11` and `unsafe-operations-impl-11-14` ask for JDK 11,
`unsafe-operations-impl-25` asks for JDK 25. Declare them in `~/.m2/toolchains.xml`:

```xml
<toolchains>
    <toolchain>
        <type>jdk</type>
        <provides><version>11</version></provides>
        <configuration><jdkHome>/path/to/jdk-11</jdkHome></configuration>
    </toolchain>
    <toolchain>
        <type>jdk</type>
        <provides><version>25</version></provides>
        <configuration><jdkHome>/path/to/jdk-25</jdkHome></configuration>
    </toolchain>
</toolchains>
```

CI installs 11, 21 and 25, and builds on 21.

## What the build checks, beyond the tests

Several things fail the build that are easy to trip over and easy to fix:

- **Javadoc references.** Javadoc runs with `-Xdoclint:all,-missing`, so a `{@link}`, `@see` or
  `@throws` that does not resolve is an error, as is raw HTML in a comment — write `{@code List<String>}`,
  not `List<String>`.
- **Licence headers.** Every source file carries the Apache header from `HEADER.txt`;
  `./mvnw license:format` adds it to a new file.
- **The module path.** `ModulePathResolutionTest` resolves the built jars as modules, so two jars may
  never contribute the same package, and every jar needs its `automatic.module.name` property. A new
  module that forgets it fails that test rather than shipping a name derived from its filename.
- **Enforcer rules**, which state the Maven and JDK minimums above, and forbid a release depending on
  a snapshot.

## Tests

- **Ring-buffer behaviour goes in `lib-impl-testkit`, never in one implementation's own tests.** It
  holds the contract as abstract JUnit classes — the single-threaded contract, and one concurrency
  test per access type — and every implementation subclasses them with its own `RingBufferBuilder`. A
  test copied into one implementation is a difference between implementations waiting to happen.
- **Do not give a module its own `<argLine>`.** It replaces the shared surefire arguments wholesale,
  and what falls out is silent: three modules once ran their tests with the buffer's head and tail
  sharing a cache line, which is not the object that ships. Add arguments with
  `surefire.argLine.extra`, or blank one named piece (`surefire.argLine.contended`,
  `.nativeAccess`, `.jdkInternalMisc`) when a module genuinely must run without it.

## Changes to the hot path

Publishing and polling are why this library exists. On those paths:

- An optional behaviour must cost nothing when it is off. The idiom is a strategy selected once —
  an interface with an active implementation and a do-nothing one — not a branch on every operation.
- Nothing allocates on the happy path: no strings, no exceptions, no collections, no boxing, no
  capturing lambda.
- If a change there is not obviously free, measure it. `benchmarks/` is a JMH suite:

  ```bash
  ./mvnw clean install -pl benchmarks -am
  java -jar benchmarks/target/benchmarks.jar
  ```

  Put the before and after numbers in the pull request.

## Pull requests

Work on a branch, open a PR against `main`, and let CI finish — it builds and tests every module on a
cold runner in about two minutes. There is no CLA: contributions are under the
[Apache License 2.0](LICENSE.txt), the same licence the project ships under.

Security problems do not go in an issue or a PR — see [SECURITY.md](SECURITY.md).
