# Ringos

Lock-free ring buffers, idling strategies and a hashed wheel timer for Java that must not allocate or block on its
hot path.

Ringos is the queueing and threading foundation under a low-latency trading stack: pre-allocated MPMC/SPSC ring
buffers whose slots are mutated in place rather than replaced, idle strategies spanning the whole CPU-burn /
wake-up-latency trade-off, a hashed wheel timer for the many-short-lived-timeouts shape that network code has, and
the low-level primitives the three of them stand on.

- **Java 11 or later**, compiled to Java 11 bytecode, tested on JDK 11 through 25.
- **No required dependencies** beyond SLF4J (and JNA, for the Linux syscalls in `ringos-clib`).
- **Apache License 2.0.**

## The four APIs

| Module | What it gives you | |
|---|---|---|
| [`lib-api`](lib-api/README.md) | Ring buffers, idle strategies, `FastThreadLocal` | [README](lib-api/README.md) |
| [`timer`](timer/README.md) | Hashed wheel timer built on a ringos MPSC buffer | [README](timer/README.md) |
| [`clib`](clib/README.md) | Linux timer slack and thread scheduling policy | [README](clib/README.md) |
| [`ringos-unsafe-operations-api`](unsafe-operations-api/README.md) | Façade over the JDK-internal `Unsafe` operations | [README](unsafe-operations-api/README.md) |

## Getting started

Ring buffers are split between an API you compile against and an implementation chosen at runtime, so you want both
coordinates — `ringos-lib-api` at compile scope and `ringos-lib-impl-all` at runtime:

```xml
<dependency>
    <groupId>org.lolaf.ringos</groupId>
    <artifactId>ringos-lib-api</artifactId>
    <version>${ringos.version}</version>
</dependency>
<dependency>
    <groupId>org.lolaf.ringos</groupId>
    <artifactId>ringos-lib-impl-all</artifactId>
    <version>${ringos.version}</version>
    <scope>runtime</scope>
</dependency>
```

A single-consumer, multi-producer buffer whose publication path allocates nothing:

```java
import org.lolaf.ringos.idling.BackoffIdleStrategy;
import org.lolaf.ringos.idling.IdleStrategy;
import org.lolaf.ringos.rb.RingBuffer;
import org.lolaf.ringos.rb.RingBufferFactory;

public final class Example {

    enum Side { BUY, SELL }

    static final class Trade {
        String symbol;
        long price;
        Side side;
    }

    // Static, so the translator captures nothing and is allocated once for the life of the process. The
    // producer passes its values as arguments rather than building a Trade for the buffer to copy from —
    // there is no source object at all, and nothing to garbage-collect afterwards.
    private static final RingBuffer.EventTranslatorThreeLongArg<Trade, String, Side> PUBLISH =
            (slot, price, symbol, side) -> {
                slot.price = price;
                slot.symbol = symbol;
                slot.side = side;
            };

    public static void main(String[] args) {
        // Every slot is pre-filled once; producers mutate those instances rather than replacing them.
        RingBuffer<Trade> buffer = RingBufferFactory.build(
                RingBufferFactory.AccessType.SINGLE_CONSUMER_MULTI_PRODUCER,
                1024,
                i -> new Trade());

        Thread consumer = new Thread(() -> {
            IdleStrategy idleStrategy = new BackoffIdleStrategy();
            while (!Thread.currentThread().isInterrupted()) {
                buffer.pollBlocking(trade -> System.out.println(trade.symbol + " " + trade.price), idleStrategy);
            }
        });
        consumer.start();

        // Nothing is allocated on this path: no Trade, no capturing lambda, no boxing.
        if (!buffer.offer(PUBLISH, 190_25L, "AAPL", Side.BUY)) {
            // The buffer is full. offerBlocking(PUBLISH, 190_25L, "AAPL", Side.BUY, idleStrategy) waits instead.
        }
    }
}
```

`EventTranslatorThreeLongArg` is the three-argument translator whose first slot is a primitive `long`, so a price
or a sequence number travels unboxed; `EventTranslatorOneArg` through `EventTranslatorFiveArg` take references.
Passing the values as arguments, instead of capturing them, is what lets `PUBLISH` be a constant rather than a
lambda allocated on every publish.

Two rules that example depends on, both spelled out in [`lib-api`](lib-api/README.md): the access type is a
contract the caller is held to, not a hint, and an element handed to a consumer belongs to the buffer only until
that consumer polls again.

## How an implementation gets chosen

Ring buffers and Unsafe operations both have several implementations, and which of them can even be *loaded*
depends on the running JDK and on the flags the JVM was started with. Both therefore follow the same shape:

- an **api** artifact you compile against (`ringos-lib-api`, `ringos-unsafe-operations-api`),
- several **impl** artifacts, each carrying a tiny `…Provider` compiled for the lowest supported class-file
  version, which names its real implementation class by `String`,
- an **`-all`** aggregator that bundles every impl, which is the one you put on the classpath.

At class-initialisation time the api walks the providers on the classpath via `ServiceLoader`, asks each whether it
claims the running JDK, keeps the winner, and only then loads the implementation class it names. Nothing that the
runtime cannot support is ever linked.

Two consequences worth knowing:

- **Never name an implementation class directly.** Go through `RingBufferFactory` and `UnsafeOperationsApi`. A
  direct reference to, say, the Java 25 implementation throws `UnsupportedClassVersionError` on JDK 21 before any
  of your code runs.
- **Degradation is graceful for ring buffers, explicit for Unsafe.** With no `--add-opens`, the ring buffer
  factory falls back to a `VarHandle`-based implementation that runs anywhere, so your application starts and
  works — just slower. `UnsafeOperationsApi` instead captures the reason it is unavailable and lets you branch
  on it.

  **How much slower depends on which buffer you use, and the spread is wide.** The fallback carries a roughly
  constant overhead per operation — measured at about four extra branches, which is what a `VarHandle` array
  access costs over a raw offset. On a many-producer buffer, where an operation is hundreds of cycles of
  coherence traffic, that disappears: about 12% at two producers and two consumers. On a
  single-producer/single-consumer buffer, where an operation is a dozen cycles, the same overhead **doubles
  it**. If your hot path is an SPSC buffer, the `--add-opens` is worth arranging.

### A second choice, for single-consumer buffers

The JDK decides *which family* of implementation you get; for `SINGLE_CONSUMER_MULTI_PRODUCER` your own call
decides which member of it. Ask for the buffer without an element instance producer and you get the slot-flagged
implementation, which reads a slot's own emptiness and is roughly 1.8× faster at handing an element over; ask with
one and you get the `…Pooled…` implementation, which publishes through a sequence array and supports the
`EventTranslator…` overloads that populate a pooled instance in place.

`RingBufferFactory` makes that choice for you, and it is the reason `offer` refuses a `null` element — see
[the lib-api README](lib-api/README.md#choosing-a-publishing-style-also-picks-an-implementation).

## JVM flags

Nothing here is required. Each flag buys back performance the JVM otherwise denies you:

```
--add-opens java.base/jdk.internal.misc=ALL-UNNAMED
-XX:-RestrictContended
-XX:ContendedPaddingWidth=64
```

| Flag | What you get | Without it |
|---|---|---|
| `--add-opens java.base/jdk.internal.misc=ALL-UNNAMED` | The `Unsafe`-backed ring buffer implementations | The `MethodHandle`/`VarHandle` implementation, which is slower but needs no flags |
| `-XX:-RestrictContended` and `-XX:ContendedPaddingWidth=64` | `@Contended` padding between the buffer's head and tail, keeping producers and consumers off one another's cache line | Head and tail share a cache line; the buffer logs an error saying so at construction |

Set `ContendedPaddingWidth` to your L1 cache line size — 64 bytes on every mainstream x86-64 and AArch64 core.

Using `ringos-unsafe-operations-api` directly needs more packages opened on older JDKs; see
[its README](unsafe-operations-api/README.md#jvm-flags) for the per-JDK table.

## Modules

Public entry points — the four with a README above:

| Artifact | Directory |
|---|---|
| `ringos-lib-api` | `lib-api` |
| `ringos-timer` | `timer` |
| `ringos-clib` | `clib` |
| `ringos-unsafe-operations-api` | `unsafe-operations-api` |

Runtime implementations, reached through the two aggregators rather than named individually:

| Artifact | Directory | Claims |
|---|---|---|
| `ringos-lib-impl-all` | `lib-impl-all` | *aggregator — depend on this one* |
| `ringos-lib-impl-unsafe` | `lib-impl-unsafe` | JDK 12+ with `jdk.internal.misc` opened |
| `ringos-lib-impl-unsafe-11` | `lib-impl-unsafe-11` | JDK 11 with `jdk.internal.misc` opened |
| `ringos-lib-impl-method-handle` | `lib-impl-method-handle` | every runtime, last resort |
| `ringos-unsafe-operations-impl-all` | `unsafe-operations-impl-all` | *aggregator — depend on this one* |
| `ringos-unsafe-operations-impl-11-14` | `unsafe-operations-impl-11-14` | JDK 11–14 |
| `ringos-unsafe-operations-impl-15-24` | `unsafe-operations-impl-15-24` | JDK 15–24 |
| `ringos-unsafe-operations-impl-25-impl` / `-provider` | `unsafe-operations-impl-25` | JDK 25+ |

And `ringos-benchmarks` (`benchmarks`), the JMH suite.

Every one of the three ring-buffer implementations is held to the same behaviour by
`ringos-lib-impl-testkit` (`lib-impl-testkit`), a test-jar of abstract JUnit classes: one for the
single-threaded contract, and one per access type that puts the buffer under load from several threads and
checks that nothing is lost, nothing is delivered twice, and each producer's elements reach each consumer in
order. An implementation module subclasses them and supplies its own `RingBufferBuilder`, so a difference
between the implementations fails the build instead of hiding in a copy of the tests that was only fixed in one
place. `lib-impl-all` subclasses the contract test too, against whatever `RingBufferFactory` selects, under both
of its surefire executions.

## Building

```bash
./mvnw -T1C clean install
```

Three modules compile against JDK internals that moved between releases, so they build through a Maven toolchain
rather than the JDK running the build: `ringos-unsafe-operations-impl-11-14` and `lib-impl-unsafe-11` ask for JDK 11,
`unsafe-operations-impl-25` asks for JDK 25. Your `~/.m2/toolchains.xml` needs entries for both, plus whichever JDK
you build with; CI installs 11, 21 and 25 and builds on 21.

To run the JMH benchmarks:

```bash
./mvnw clean install -pl benchmarks -am
java -jar benchmarks/target/benchmarks.jar
```

## License

Apache License 2.0 — see [LICENSE.txt](LICENSE.txt).
