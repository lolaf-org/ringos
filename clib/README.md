# ringos-clib

The handful of Linux system calls ringos needs to make a thread behave predictably: the **timer slack** that decides
how late a short park may return, and the **scheduling policy** that decides whether the thread is preempted at all.

```xml
<dependency>
    <groupId>org.lolaf.ringos</groupId>
    <artifactId>ringos-clib</artifactId>
    <version>${ringos.version}</version>
</dependency>
```

Bound through JNA (`prctl(2)`, `sched_setscheduler(2)`). No JVM flags, no `--add-opens`.

`ringos-lib-api` already depends on this, for `TimerSlackAwareBackoffIdleStrategy`. You only need it directly to
retune a thread of your own.

## The API is three methods

```java
CLibrary clib = CLibraryApi.get();   // never null, always the same stateless instance

Duration slack = clib.getTimerSlack();
int errno = clib.setTimerSlack(Duration.ofNanos(5_000));
int errno = clib.setThreadScheduler(50, LinuxScheduler.SCHED_FIFO);
```

Never construct an implementation — `CLibraryApi.get()` is the only way in.

## The no-op fallback, and what it costs you

Off Linux, or on Linux when the C library could not be loaded, **every method is a no-op returning the neutral
value**. The `default` methods on the `CLibrary` interface *are* that implementation; the Linux-backed one
overrides them all. Loading fails more often than you would guess — a container whose tmpdir is mounted `noexec`
stops JNA unpacking its own native library — and it degrades rather than throwing, so a caller never has to ask
whether the platform supports any of this.

The price is that success and silence look alike:

| | Real call | No-op |
|---|---|---|
| `getTimerSlack()` | the thread's slack, typically 50 µs | `Duration.ZERO` |
| `setTimerSlack(...)` | `0`, or an `errno` | `0` |
| `setThreadScheduler(...)` | `0`, or an `errno` | `0` |

`Duration.ZERO` is unambiguous — a real thread never has zero slack, so zero means *unknown*, not *perfectly
precise*, and it is the signal to leave slack alone. The two setters are not: a `0` means "nothing went wrong",
never "the setting took". **Read the slack back with `getTimerSlack()` if it matters.**

## Everything here is per-thread

Timer slack and scheduling policy are properties of the **calling thread** on Linux, not of the process. Calling
either one retunes that one thread and leaves every other thread in the JVM alone.

So call them *from* the thread you mean to privilege — the polling loop itself — the way
`TimerSlackAwareBackoffIdleStrategy` narrows slack from `assignToThread`, on the thread that is about to idle.

The one leak in that isolation: a new thread inherits the policy of the thread that created it. Retune a thread
that goes on to spawn others and they all start out retuned — worth knowing before doing this on a thread that
owns a pool.

## Timer slack

The kernel rounds a timer or a short `park` up by the calling thread's timer slack, 50 µs by default. That is the
whole reason `BackoffIdleStrategy`'s minimum park period is also 50 µs: anything shorter would be rounded back up,
so it would buy nothing.

```java
clib.setTimerSlack(Duration.ofNanos(5_000));   // 5 µs parks are now actually honoured
```

The setting applies to the thread and outlives any particular use of it. A value of zero restores the inherited
default rather than removing slack altogether.

## Scheduling policy

```java
int errno = clib.setThreadScheduler(50, LinuxScheduler.SCHED_FIFO);
if (errno == CLibrary.EPERM) {
    // no CAP_SYS_NICE — see below
}
```

| Policy | |
|---|---|
| `SCHED_OTHER` | The default, CFS. Every thread gets a share and is preempted to give the others theirs. Priority is always 0; niceness biases the share. |
| `SCHED_FIFO` | Real-time. Runs until it blocks or a higher-priority thread appears — **the one to give a latency-critical polling thread**, and the one that will monopolise a core if that thread never yields. |
| `SCHED_RR` | Real-time, round-robin. Same priority band as FIFO, but same-priority threads take turns on a time slice. |
| `SCHED_BATCH` | Preempted less eagerly. Suits batch work, hurts anything latency-sensitive. |
| `SCHED_IDLE` | Runs only when nothing else wants the CPU. For background threads that must never take a cycle from the message path. |

Priority is 1–99 for the real-time policies and 0 for the others; higher preempts lower.

Two policies are deliberately absent. `SCHED_DEADLINE` cannot be set through `sched_setscheduler` at all — it needs
the runtime, period and deadline that only `sched_setattr` carries, and the call rejects it with `EINVAL` — so
offering it would only hand you a constant that always fails. `SCHED_ISO` was never implemented in mainline Linux.

### The real-time policies need `CAP_SYS_NICE`

Without it the call returns `EPERM`. Grant it to the JVM binary, the container, or the systemd unit:

```bash
# one JVM, on the host
sudo setcap cap_sys_nice+ep "$JAVA_HOME/bin/java"

# a container
docker run --cap-add=SYS_NICE ...

# Kubernetes, in the container's securityContext
securityContext:
  capabilities:
    add: ["SYS_NICE"]

# systemd unit
AmbientCapabilities=CAP_SYS_NICE
```

Raise the policy deliberately, and on a thread you know yields: a real-time thread that spins without ever blocking
can lock up a core against everything else on the machine.

The two `errno`s worth handling are exposed as constants — `CLibrary.EPERM` (1) for want of privilege, and
`CLibrary.EINVAL` (22) for a priority outside the policy's range or a policy this call cannot configure.

## See also

- [`prctl(2)`](https://man7.org/linux/man-pages/man2/prctl.2.html) — `PR_SET_TIMERSLACK`, `PR_GET_TIMERSLACK`
- [`sched_setscheduler(2)`](https://man7.org/linux/man-pages/man2/sched_setscheduler.2.html)
- [`sched(7)`](https://man7.org/linux/man-pages/man7/sched.7.html) — the policies in full
- `TimerSlackAwareBackoffIdleStrategy` in [`ringos-lib-api`](../lib-api/README.md#backoff-the-default), the in-tree
  consumer of all this
