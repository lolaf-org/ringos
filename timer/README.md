# ringos-timer

A hashed wheel timer — a variant of Varghese & Lauck (1987), modeled after Netty's `HashedWheelTimer` but using a
ringos MPSC ring buffer for the cross-thread submission path, so `schedule` is lock-free and allocates nothing
beyond the `Timeout` handle. `newReusableTimeout()` removes even that.

```xml
<dependency>
    <groupId>org.lolaf.ringos</groupId>
    <artifactId>ringos-timer</artifactId>
    <version>${ringos.version}</version>
</dependency>
<dependency>
    <groupId>org.lolaf.ringos</groupId>
    <artifactId>ringos-lib-impl-all</artifactId>
    <version>${ringos.version}</version>
    <scope>runtime</scope>
</dependency>
```

It pulls in `ringos-lib-api` transitively; the ring buffer implementation is still yours to put on the runtime
classpath.

## When a wheel beats `ScheduledExecutorService`

A wheel is an array of buckets and a rotating cursor. Scheduling is a modulo and an append to a bucket — O(1),
whatever the delay — and cancellation is a flag, not a heap removal; the node is unlinked for free on the next
walk of its bucket. A `DelayQueue`-backed executor pays
O(log n) on both, and reorders a priority heap on every operation.

That trade is worth taking for the shape network code has: **many short-lived timeouts, most of them cancelled
rather than fired.** Every message you send arms a response timeout you expect to cancel. It is the wrong trade for
a handful of long-lived periodic jobs, where the executor's exact scheduling is worth more than the O(1).

The wheel buys that with coarser resolution: a task fires on the first tick at or after its deadline, never before.

## Sizing

```java
WheelTimer timer = new WheelTimer(
        TimeUnit.MILLISECONDS.toNanos(1),  // tickDurationNanos
        512,                               // wheelSize — power of two
        1024,                              // submissionQueueSize — power of two
        true);                             // multiThreaded
```

- **`tickDurationNanos`** is your resolution floor.
- **`wheelSize`** × tick duration is one revolution. Timeouts longer than a revolution are handled, but they sit in
  a bucket for several rotations (a `remainingRounds` counter decremented on each walk), so a wheel comfortably
  longer than your typical timeout keeps buckets short and those walks cheap.
  Above, 512 × 1 ms = 512 ms.
- **`submissionQueueSize`** caps how many schedules can be in flight between two ticks. Overflow is what makes a
  `Timeout` come back rejected.

All three must be positive, and both sizes powers of two; the constructor throws `IllegalArgumentException`
otherwise.

**Effective resolution is `max(tickDurationNanos, idle-strategy granularity)`** — a 1 µs tick means nothing behind
a strategy that parks for 50 µs. For sub-microsecond ticks pass a `BusySpinIdleStrategy`; for ordinary network
timeouts a `BackoffIdleStrategy` or `TimerSlackAwareBackoffIdleStrategy` is the right shape.

## Two execution modes

**Its own worker thread**, ticking in a loop interleaved with an idle strategy:

```java
timer.startOwnThread(new BackoffIdleStrategy(), new NamedThreadFactory("timer"));
```

`WaitNotifyIdleStrategy` is rejected here with an `IllegalArgumentException`: it blocks indefinitely on
`Object.wait()` and would stop the wheel advancing between submissions. Use `TimedWaitNotifyIdleStrategy(maxWait)`
if you want that shape. The worker gets a logging uncaught-exception handler unless the factory already set one.

**Driven from a loop you already have** — a select loop, a poll loop, anything single-threaded:

```java
timer.start();
while (running) {
    int io = pollIo();
    int fired = timer.tick();       // returns how many tasks fired
    idleStrategy.idle(io + fired);  // feed the work count back into the backoff
}
```

`tick()` advances the wheel to the current `System.nanoTime()`, catching up by walking however many ticks have
elapsed since the last call. An irregular cadence is therefore fine — you get several ticks at once, not drift.
It is also safe before `start()` and after `stop()`, returning `0` without touching the wheel, so a select loop can
call it across the timer's whole lifecycle.

`start()` resets the time origin, so a timer restarted after `stop()` counts from zero again.

## Threading contract

| | Rule |
|---|---|
| `schedule(...)` | Any thread **only** when built with `multiThreaded = true`, which selects an MPSC submission buffer. With `false` it is SPSC and **one** thread must own every schedule call. |
| `tick()` | A single thread for the lifetime of the timer — the wheel arrays are not synchronized. |
| `stop(Duration)` | Not from the worker or tick thread; it joins that thread. Safe to call concurrently. |

`start`, `startOwnThread` and `stop` are synchronized on the timer, so lifecycle transitions run one at a time.
`schedule` and `tick` take no lock.

## Shutdown

`stop(Duration)` cancels everything pending or in flight and rejects every later `schedule`. It is idempotent, and
concurrent calls are safe — exactly one performs the shutdown and the others return immediately.

**Who does the cancelling depends on the mode**, because the wheel arrays and the submission buffer are
single-consumer and may only be touched from the tick thread:

- **Own-thread mode** — `stop` interrupts the worker and joins it, and the *worker* cancels the outstanding tasks
  on its way out. Values under 10 ms are raised to 10 ms. If the worker overruns the timeout — wedged in a task
  that ignores interrupts, say — `stop` logs an error and returns anyway rather than draining a wheel the worker
  still owns; those tasks stay scheduled until the worker does exit, at which point it cancels them itself.
- **Tick-driven mode** — stop calling `tick()` *before* calling `stop`, which then cancels on your thread.

Cancelling drains the submission buffer for up to 100 ms rather than in a single pass. A producer that has
claimed a slot but not yet published it makes the buffer look empty at that slot, and anything already published
behind it would otherwise sit there for ever — never run, and never reported to whoever scheduled it. The wait is
for an `offer` already in flight, not for producers to stop: every `schedule` from here on is rejected.

A worker that dies of its own accord — an idle strategy that throws, say — marks the timer stopped as it unwinds,
so later `schedule` calls are rejected instead of queueing into a wheel nothing will walk.

## Rejection is a normal outcome

```java
Timeout timeout = timer.schedule(task, 30, TimeUnit.SECONDS);
if (timeout.isRejected()) {
    // never reached the wheel, and is guaranteed never to fire — retry or surface it
}
```

A schedule is rejected when the submission buffer was full at call time, or when the timer was not started.
`schedule` deliberately does **not** block: it is the cross-thread fast path, and back-pressure is handed to the
caller rather than absorbed silently.

The `Timeout` handle also carries `getDeadlineNanos()`, `cancel()`, `isCancelled()` and `isExpired()`.
**Cancellation is cooperative**: `cancel()` only flags the timeout. The unlinking happens on the timer thread when
the bucket is next visited, so a cancelled timeout never runs but may stay briefly referenced.

## Scheduling without allocating

Two allocations hide in a naive `schedule(() -> handleTimeout(order), 30, SECONDS)`: the capturing lambda, and the
`Timeout` handle. Both are avoidable.

**The capturing lambda** — hold the task in a `static final` field and pass its arguments separately, exactly as
ring buffer event translators do. `TimerTaskOneArg`, `TimerTaskTwoArg` and `TimerTaskThreeArg` exist for this:

```java
private static final TimerTaskTwoArg<Session, Order> ON_TIMEOUT = (session, order) -> session.expire(order);

timer.schedule(ON_TIMEOUT, session, order, 30, TimeUnit.SECONDS);
```

**The handle** — allocate it once and re-arm it:

```java
private final MutableTimeout handle = timer.newReusableTimeout();

timer.schedule(handle, ON_TIMEOUT, session, order, 30, TimeUnit.SECONDS);
```

Recycling is only safe once the previous schedule reached a terminal state — `isExpired()` after firing, or
`isRejected()`. Re-arming a handle whose previous use is still in flight, **or was cancelled**, throws
`IllegalStateException`: there is no barrier guaranteeing the worker has finished with a cancelled node, so reuse
would race. That means a cancel-heavy path cannot recycle handles, which is usually the deciding factor in whether
this optimisation applies to you.

**Re-arming from inside the task is allowed**, and is how a repeating timer is built:

```java
private static final TimerTaskOneArg<Session> HEARTBEAT = session -> {
    session.sendHeartbeat();
    session.timer().schedule(session.handle(), HEARTBEAT, session, 1, TimeUnit.SECONDS);
};
```

That runs on the worker thread, after the node has left its bucket and after the worker has taken its own copy of
what it still needs — so the task that just ran still gets its own completion callback, and the handle goes on to
its next cycle.

## Task execution and failures

Tasks run on the tick/worker thread, so they must be cheap and non-blocking — anything slow belongs on a ring
buffer handed to another thread.

**The per-task completion callback**, available on the `Runnable` overloads, is a
`BiConsumer<Runnable, Exception>` run immediately after the task on the same thread, exactly once, with
`(task, null)` on success or `(task, exception)` when it threw. It is not called when the task was cancelled or
when scheduling was rejected.

**`TimerTaskExceptionHandler` is the backstop**, and the only report of a failure for the
`TimerTaskOneArg`/`TwoArg`/`ThreeArg` overloads, which carry no per-task callback:

```java
timer.setTaskExceptionHandler((task, exception) -> metrics.timerTaskFailed(task, exception));
```

The default logs at error level, so a failing task is never silent. The handler also receives:

- an `Error` thrown by a task, which the worker swallows rather than dying on;
- a `Throwable` thrown by a completion callback — which is never handed back to that callback as if the task
  itself had failed.

Set it before `start()`; it runs on the tick thread under the same cheap-and-non-blocking rule, and a handler that
throws has its own `Throwable` discarded, there being nowhere left to report it.

`isExpired()` becomes true only once the task **and** its callback have finished, so a handle that reports expired
is genuinely done with the worker. A timeout the worker has claimed but not finished reports `false` from both
`isExpired()` and `isCancelled()`, and `cancel()` on it returns `false` — it is past the point of being cancelled.
