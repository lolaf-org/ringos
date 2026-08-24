# unsafe-operations-api

A narrow façade over the JDK-internal `Unsafe` operations ringos needs: direct-buffer deallocation, field and array
layout offsets, raw access at those offsets, and the L1 cache line width.

```xml
<dependency>
    <groupId>org.lolaf.ringos</groupId>
    <artifactId>ringos-unsafe-operations-api</artifactId>
    <version>${ringos.version}</version>
</dependency>
<dependency>
    <groupId>org.lolaf.ringos</groupId>
    <artifactId>ringos-unsafe-operations-impl-all</artifactId>
    <version>${ringos.version}</version>
    <scope>runtime</scope>
</dependency>
```

`ringos-unsafe-operations-impl-all` bundles the JDK 11–14, 15–24 and 25+ implementations. Depending on one of them
individually only works if you control the JDK your code runs on.

## Availability is an outcome, not an exception

Unsafe is genuinely unusable on some runtimes: a required `--add-opens` is missing, the JDK is outside every
supported range, or no implementation is on the classpath. So `UnsafeOperationsApi` captures the reason at
class-initialisation instead of throwing it. **Merely touching the class never fails** — you choose how to react.

```java
// 1. Have a fallback? Compute through it, and name the default.
int lineSize = UnsafeOperationsApi.ifAvailableDoReturn(
        UnsafeOperations::getL1CacheLineSize,
        UnsafeOperations.DEFAULT_L1_CACHE_LINE_SIZE);

// 2. Purely an optimisation or a diagnostic? Skip it silently.
UnsafeOperationsApi.ifAvailableDo(unsafe -> unsafe.invokeCleanerIfNeeded(buffer));
UnsafeOperationsApi.ifAvailableDo(UnsafeOperations::invokeCleanerIfNeeded, buffer);  // no capturing lambda

// 3. Branch explicitly.
if (UnsafeOperationsApi.isAvailable()) { ... }

// 4. No fallback exists? Take it or blow up.
UnsafeOperations unsafe = UnsafeOperationsApi.get();
```

`ifAvailableDo`'s no-op case is **silent**, so use it only where skipping the work is genuinely harmless.
`get()` throws wrapping the captured cause — a `MissingAddOpensException` naming the exact flags, a
`WrongJDKException` listing the supported ranges, or whatever stopped the implementation loading.

Never name an implementation class directly. Which of them can even be *loaded* depends on the running JDK — the
Java 25 implementation is compiled with `--release 25` and throws `UnsupportedClassVersionError` on JDK 21 before
any of your code runs. That is the whole reason for the provider SPI; see
[the root README](../README.md#how-an-implementation-gets-chosen).

## JVM flags

| Running JDK | Packages to open |
|---|---|
| 11–14 | `jdk.internal.misc`, `jdk.internal.ref`, `sun.nio.ch` |
| 15–24 | `jdk.internal.misc` |
| 25+ | `jdk.internal.misc` |

Each as `--add-opens java.base/<package>=ALL-UNNAMED`. On JDK 11–14 that is:

```
--add-opens java.base/jdk.internal.misc=ALL-UNNAMED
--add-opens java.base/jdk.internal.ref=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
```

The two extra packages on 11–14 are for reaching a direct buffer's cleaner; from 15 onwards `Unsafe.invokeCleaner`
covers it. If you run without the flags, `MissingAddOpensException`'s message lists exactly what to paste onto the
command line.

## Safety contract

These methods bypass every check the language gives you. **There is no exception to catch.**

- **Offsets must come from this API, for the very object being accessed.** Anything else corrupts memory or crashes
  the JVM.
- **Offsets are valid for one JVM run and one class.** Cache them in a `static final` next to the class they
  describe; never persist or share them.
- **Only `getAndSetInt` and `getAndSetReference` are atomic and ordered** (volatile semantics on both halves).
  Every `get*` / `put*` pair is a **plain** access: no happens-before edge, so a value one thread writes may never
  become visible to another. Use them single-threaded, or where some other fence establishes the ordering.
- `putLong` is not even atomic on a 32-bit VM.
- Implementations are stateless and safe to share across threads.

The idiomatic shape:

```java
private static final long HEAD_OFFSET =
        UnsafeOperationsApi.get().objectFieldOffset(MyBuffer.class, "head");
```

## What is in the façade

### Direct buffer deallocation

```java
void invokeCleanerIfNeeded(ByteBuffer buffer);  // frees if direct, ignores heap buffers
void invokeCleaner(ByteBuffer buffer);          // throws on a heap buffer
```

The memory is gone when the call returns. Any later read or write through that buffer — **or through a slice or
duplicate of it** — touches freed memory and can crash the JVM. Only call this on a buffer you own outright and
will not hand out again.

### Layout offsets

```java
long objectFieldOffset(Class<?> clazz, String field);  // UNKNOWN_FIELD_OFFSET (-1) if not declared
long objectFieldOffset(Field field);                   // throws instead of a sentinel
long arrayBaseOffset(Class<?> arrayClass);
int  arrayIndexScale(Class<?> arrayClass);
```

The name lookup covers **declared** fields only — an inherited field is not found — and returns
`UNKNOWN_FIELD_OFFSET`, deliberately negative so using it unchecked fails loudly rather than reading a neighbouring
field. Test for it: the JDK is free to rename or hide its internals between releases.

Element `i` of an array sits at `arrayBaseOffset + (long) i * arrayIndexScale`. The scale for a reference array is
4 or 8 depending on whether compressed oops are on, which is why it must be read rather than assumed.

### Raw access

```java
<T> T getAndSetReference(Object obj, long offset, T value);   // atomic, volatile ordering
int     getAndSetInt(Object obj, long offset, int value);     // atomic, volatile ordering

<T> T getReference(Object obj, long offset);                  // plain
void  putReference(Object obj, long offset, Object value);    // plain
void  putInt(Object obj, long offset, int value);             // plain
void  putLong(Object obj, long offset, long value);           // plain
```

`getReference` is unchecked, so a wrong `T` surfaces as a `ClassCastException` at the assignment rather than at the
call. `putReference` does not check assignability against the slot's declared type.

### Cache line width

```java
int getL1CacheLineSize();
```

What the runtime can report varies: on Java 11–14 there is no way to ask, so `DEFAULT_L1_CACHE_LINE_SIZE` (64) comes
back; from Java 15 it is the CPU's real flush size, which is `0` on a CPU that does not report one. **Treat any
non-positive result as 64** — or just use `ifAvailableDoReturn` with that default, which handles both the
unavailable case and the JDK 11–14 case in one line.

## Adding a JDK range

If a future JDK moves these internals again, add a module with:

- an implementation of `UnsafeOperations` compiled for that release;
- an implementation of `UnsafeOperationsProvider` compiled to the **lowest** supported class-file version — it has
  to load on every JDK for `ServiceLoader` to be able to ask it anything — which names the implementation only as a
  `String` from `implementationClassName()`, never as a typed reference;
- `isForCurrentJDK()` and `requiredOpenPackages()` for the new range;
- the module added to `ringos-unsafe-operations-impl-all`.

The Java 25 module shows the split: `unsafe-operations-impl-25/impl` targets 25, `…/provider` is compiled at 11.
