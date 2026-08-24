# ringos-lib-api

The ring buffers, the idle strategies that drive their polling loops, and the fast thread-local machinery around
them.

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

This artifact holds interfaces and abstract bases only. Without `ringos-lib-impl-all` on the runtime classpath,
`RingBufferFactory` fails at class-initialisation saying it found no provider at all. See
[the root README](../README.md#how-an-implementation-gets-chosen) for why the split exists.

---

## Ring buffers

A `RingBuffer<T>` is pre-allocated, fixed-capacity and never grows. `offer` returns `false` when full, `poll`
returns `null` when empty; neither blocks nor allocates. Capacity is a power of two, fixed at construction.

Everything comes from `RingBufferFactory.build(...)`.

### Pick the access type honestly

```java
import static org.lolaf.ringos.rb.RingBufferFactory.AccessType.SINGLE_CONSUMER_MULTI_PRODUCER;

RingBuffer<Trade> buffer = RingBufferFactory.build(SINGLE_CONSUMER_MULTI_PRODUCER, 1024, i -> new Trade());
```

| `AccessType` | `poll()` from | `offer()` from |
|---|---|---|
| `SINGLE_CONSUMER_SINGLE_PRODUCER` | one thread | one thread |
| `SINGLE_CONSUMER_MULTI_PRODUCER` | one thread | any number |
| `MULTI_CONSUMER_SINGLE_PRODUCER` | any number | one thread |
| `MULTI_CONSUMER_MULTI_PRODUCER` | any number | any number |

**This is a contract, not a hint.** The narrower variants skip coordination the wider ones pay for, which is the
whole point of choosing one — and a buffer used more concurrently than it was declared corrupts silently rather
than throwing. Pick the narrowest type that describes the actual callers, and revisit it when the callers change.

### Two ways to publish

**Event translators — the zero-allocation path.** Build the buffer with an element instance producer and every slot
is pre-filled once with a long-lived instance. Producers then hand the buffer a translator, which is called with
the slot's own element to populate in place:

```java
// static, so the translator captures nothing and is allocated once for the life of the process
private static final RingBuffer.EventTranslatorThreeLongArg<Trade, String, Side> PUBLISH =
        (slot, price, symbol, side) -> {
            slot.price = price;
            slot.symbol = symbol;
            slot.side = side;
        };

buffer.offer(PUBLISH, 190_25L, "AAPL", Side.BUY);
```

The translator runs while the slot is still private to the publishing producer, and the element becomes visible to
consumers only once it returns. It is not called at all when the buffer is full, so a failed publish costs nothing
but the failed claim. Overloads take one to five arguments — `EventTranslatorOneArg` … `EventTranslatorFiveArg`,
plus the `EventTranslatorThreeLongArg` used above, whose first argument is a primitive `long` so publishing a price
or a sequence number does not box it. Passing the values as arguments is what lets the translator stay static
instead of becoming a capturing lambda allocated on every publish.

**`offer(T)` — the reference path.** Stores the reference you give it, replacing whatever the slot held:

```java
buffer.offer(trade);
```

Use it on a buffer built *without* an element instance producer. On a pre-filled buffer it throws away the pooled
instance the slot was holding, defeating the pooling. And once it returns, the element is visible to a consumer —
so do not mutate it afterwards.

### Choosing a publishing style also picks an implementation

For `SINGLE_CONSUMER_MULTI_PRODUCER` the two styles above are not merely different APIs over one buffer. Whether
you pass an element instance producer decides which implementation `RingBufferFactory` hands back, and they carry
their elements very differently:

| built with | implementation | how a full slot is recognised | translators |
|---|---|---|---|
| no element producer | `…MpScRingBuffer` | the slot's own contents — empty means empty | unsupported |
| an element producer | `…PooledMpScRingBuffer` | a sequence number in an array beside the slot | supported |

**The first is roughly 1.8× faster**, measured with two producers and one consumer pinned to one CCD. The reason
is cache lines, not instructions: handing an element from a producer to a consumer moves the slot between their
cores either way, and the sequenced implementation moves a second line — the slot's sequence, written by both
sides — for every element. Carrying the same information in the slot itself halves that traffic.

A pre-filled slot is never empty, so it cannot say "nothing here", which is why a buffer built with an element
producer keeps the sequenced implementation and why the `EventTranslator…` overloads throw
`UnsupportedOperationException` on the other one. That is also why `offer` rejects `null`: a stored `null` would
be indistinguishable from a free slot.

So the zero-allocation path and the cheapest hand-off are a genuine trade, not a free win. Pool instances when
the elements are large or allocation is what you are avoiding; leave the producer off when the hand-off itself is
the cost that matters. The other three access types are unaffected — they use the sequenced implementation either
way.

### Element ownership

An element handed to a consumer stays owned by the buffer. It is valid until that consumer polls again, after which
a producer may overwrite it in place. **Anything you keep beyond the callback must be copied out.** This applies to
`poll(Consumer)`, `drain`, `poll(Consumer[])` and the blocking variants alike.

`poll(Consumer)` holds the slot until the consumer returns, so a slow consumer holds the head back rather than
exposing a half-read element to a producer.

### Consuming

```java
T poll();                                              // null if empty
boolean poll(Consumer<T> consumer);                    // false if empty
void drain(Consumer<T> consumer);                      // until empty, one poll at a time
int poll(Consumer<T>[] consumers);                     // batch: one head update for the whole batch
T peek();                                              // no removal
```

`poll(Consumer[])` claims up to `consumers.length` elements with a single head update, which is what makes it
cheaper than the equivalent run of single polls; the n-th element removed goes to `consumers[n]`.

`drain` judges emptiness one poll at a time, so against a producer that keeps up it drains for as long as the
producer publishes rather than stopping at a snapshot.

### Blocking variants

Every `poll` and `offer` has an `…Blocking` form taking an `IdleStrategy`, which turns a refusal into a wait:

```java
Trade trade = buffer.pollBlocking(idleStrategy);                        // waits indefinitely
Trade trade = buffer.pollBlocking(idleStrategy, Duration.ofMillis(10)); // null if it stayed empty
buffer.pollBlocking(consumer, idleStrategy);
buffer.offerBlocking(trade, idleStrategy);
buffer.offerBlocking(PUBLISH, 190_25L, "AAPL", Side.BUY, idleStrategy);
```

`reset()` is called on the strategy before the first idle, and not at all when the operation succeeds straight
away. The `Duration` variant checks the clock between idles, so it bounds the wait only as tightly as the
strategy's idle period allows.

### Accessors are estimates

`getSize()`, `isEmpty()`, `isNotEmpty()`, `isFull()`, `getCurrentHead()` and `getCurrentTail()` are point-in-time
estimates the moment another thread is running. They are for monitoring and sizing decisions. **Never guard an
operation with them** — only the return value of the `offer` or `poll` itself says whether it happened.

### Cache-line padding

The head and tail cursors are `@Contended`, so producers and consumers do not fight over one cache line. That
annotation needs `-XX:-RestrictContended -XX:ContendedPaddingWidth=64`; without them the buffer logs an error at
construction saying how many bytes apart head and tail actually landed.

Separately, `bufferPaddingEnabled` pads both ends of the backing arrays, keeping the buffer's own elements off the
cache lines of whatever the allocator placed next to them. It defaults to the
`org.lolaf.ringos.rb.RingBufferFactory.defaultBufferPaddingEnabled` system property, read once at class
initialisation; the `build` overloads taking a `boolean` decide per buffer instead.

---

## Idle strategies

An `IdleStrategy` is what a polling thread does when it finds nothing — the one knob trading CPU burn against
wake-up latency.

| Strategy | Wake-up | CPU while idle | Use when |
|---|---|---|---|
| `BusySpinIdleStrategy` | tens of ns | a full core, always | the thread owns its core and the latency is worth the burn |
| `YieldingIdleStrategy` | a scheduler decision | still burns, but shares | you want to stop monopolising the core without ever parking |
| `BackoffIdleStrategy` | ns → µs → ms as it escalates | near zero once quiet | **the general-purpose default** |
| `TimerSlackAwareBackoffIdleStrategy` | as above, honouring sub-50 µs parks | near zero once quiet | a park of a few microseconds is genuinely the point (Linux only) |
| `WaitNotifyIdleStrategy` | when a producer calls `wakeup()` | none | throughput matters and someone will always signal |
| `TimedWaitNotifyIdleStrategy` | on `wakeup()`, or when the bound elapses | none | as above, but the thread must also progress on its own schedule |

### Backoff, the default

`BackoffIdleStrategy` escalates through three phases: 10 spins, then 5 yields, then a park that doubles from 50 µs
up to 1 ms. Any progress rewinds it — `idle(workCount)` with a non-zero count calls `reset()` — so a burst of work
leaves the strategy spinning again rather than parked at its longest period.

```java
new BackoffIdleStrategy();                                        // the DEFAULT_ constants above
new BackoffIdleStrategy(maxSpins, maxYields, minParkNs, maxParkNs);
```

The 50 µs floor is not arbitrary: it is roughly the kernel's default timer slack, so a shorter park would be
rounded back up anyway. Going below it takes `TimerSlackAwareBackoffIdleStrategy`, which narrows the slack on the
idling thread first — via [`ringos-clib`](../clib/README.md), and only on Linux. The no-argument constructor buys
nothing over the parent, since its default minimum park already equals the default slack; use the four-argument one.

### Ownership

A strategy carries the backoff state of the one thread idling on it, so **give each idling thread its own**. The
two stateless ones are the exception and expose singletons: `BusySpinIdleStrategy.getInstance()` and
`YieldingIdleStrategy.getInstance()`.

`wakeup()` is the exception to that ownership — it is meant to be called by producers, from other threads. It is a
no-op on the strategies that never park.

`assignToThread(Thread)` is called by the idling thread on itself before its loop starts; most strategies need
nothing, and `TimerSlackAwareBackoffIdleStrategy` uses it to narrow the OS timer slack.

---

## Threading

### `FastThreadLocal`

A `ThreadLocal` resolved by array index instead of a hash-map probe, which makes a lookup around 50% faster — on a
`FastThreadLocalThread`. On a plain thread it still works, falling back to a regular `ThreadLocal`, but the point
of the class does not apply.

```java
private static final FastThreadLocal<StringBuilder> BUFFER =
        FastThreadLocal.withInitial(() -> new StringBuilder(256));
```

**`static final` is a requirement, not a style preference.** Each instance claims an index at construction out of a
global space that is never reclaimed, and every thread's values live in one array addressed by those indices.
Creating them per request leaks index space and grows every thread's array.

Values survive until `remove()`, `removeAll()`, or the thread finishing a body that a `FastThreadLocalThread`
wrapped. A thread that outlives the values it holds — a pooled one above all — must be cleaned explicitly, or it
pins whatever they reference.

### `FastThreadLocalThread`

The thread that makes the array lookup possible: it carries the value array itself, and wraps the body you give it
so that `FastThreadLocal.removeAll()` runs when that body returns. That is what keeps a pooled or recycled thread
from carrying stale values — and their memory — into its next use.

Constructors mirror `Thread`'s: `(target)`, `(group, target)`, `(target, name)`, `(group, target, name)` and
`(group, target, name, stackSize)`.

### `NamedThreadFactory`

A `ThreadFactory` whose threads are named `prefix-N`, so a thread dump or a profiler attributes work to the
component that spawned it:

```java
new NamedThreadFactory("md-worker");         // daemon
new NamedThreadFactory("md-worker", false);  // keeps the JVM alive
```

Two further differences from `Executors.defaultThreadFactory()`: the threads are daemons by default, and each
carries an uncaught-exception handler that logs at error level — without which a thread dying in a polling loop
would leave nothing behind but a stalled queue.

The threads are plain `Thread`s. `FastThreadLocal` users need `FastThreadLocalThread` instead.

### `CpuTopology`

Which CPUs share a last-level cache, and which share a physical core, read from `sysfs`. It answers the one
question that decides where a producer and a consumer should run: do they share a cache, or does every handoff
between them have to cross an interconnect?

```java
CpuTopology topology = CpuTopology.detect().orElseThrow();

topology.lastLevelCacheDomainCount();                       // 2 on a two-die part
topology.shareLastLevelCache(List.of(6, 7, 8, 9));          // the check worth making
topology.smtSiblingCollisions(List.of(6, 18));              // [6, 18] — one physical core
topology.lastLevelCacheBytesOf(0);                          // tells two dies apart
```

**On a chiplet CPU the cores of one socket are split across dies that share nothing below main memory.** A ring
buffer whose two ends land on different dies pays up to double the cost of one whose ends share a last-level
cache — with nothing in the JVM, the thread names or the affinity call to say it happened. Pinning threads
without checking this is how a benchmark ends up measuring an interconnect instead of a queue.

The obvious ways to look for that split do not find it. NUMA typically reports a single node for such a socket,
`topology/cluster_id` is frequently unset, and `topology/die_id` is only as reliable as the vendor's ACPI tables.
What holds across machines is the cache hierarchy itself, which is what this reads.

Everything degrades to an empty result rather than a wrong one: `detect()` yields an empty `Optional` wherever
`sysfs` is absent, and the per-CPU queries yield empty for a CPU whose files could not be parsed.
`shareLastLevelCache` answers `false` for a CPU it knows nothing about, since an unproven placement is not a good
one. Nothing is cached, and nothing belongs on a hot path.

It reports what `sysfs` exposes, which is the **host's** layout — inside a container that is not the set of CPUs
the process may actually use, so intersect it with the real affinity mask before drawing conclusions.

### `Deadline`

A time budget handed down a chain of calls, so each step waits on what is left of the original allowance rather
than on a fresh timeout of its own.

```java
Deadline deadline = Deadline.of(Duration.ofMillis(50));
Deadline unlimited = Deadline.unlimited();   // never expires
Deadline immediate = Deadline.immediate();   // already has
```

The unlimited and immediate forms are what let a caller say "block as long as it takes" and "don't block at all"
without the callee special-casing either — every accessor answers sensibly for all three.
`fromRemainingTime(double)` carves a fraction of what is left off for a sub-step; `pause()` / `resume()` stop the
clock across a stretch that should not be charged to it. Elapsed time is measured from construction, so create a
deadline where the budget starts, not where it is used. Not thread-safe.
