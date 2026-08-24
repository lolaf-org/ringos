/*
 * Copyright © 2024-2026 Lolaf.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lolaf.ringos.timer;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.lolaf.ringos.idling.IdleStrategy;
import org.lolaf.ringos.idling.WaitNotifyIdleStrategy;
import org.lolaf.ringos.rb.RingBuffer;
import org.lolaf.ringos.rb.RingBufferFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Hashed wheel timer. Variant of Varghese &amp; Lauck (1987), modeled after Netty's
 * {@code HashedWheelTimer}, but using a ringos MPSC ring buffer for the cross-thread
 * submission path so {@link #schedule} is lock-free and zero-allocation aside from the
 * {@link Timeout} handle itself — and {@link #newReusableTimeout()} removes even that.
 * <p>
 * <b>Resolution.</b> The effective resolution is {@code max(tickDurationNanos, idleStrategy
 * granularity)}. For sub-microsecond ticks pass a {@link org.lolaf.ringos.idling.BusySpinIdleStrategy}
 * via {@link #startOwnThread}; for typical low-latency network timeouts a
 * {@link org.lolaf.ringos.idling.BackoffIdleStrategy} or
 * {@link org.lolaf.ringos.idling.TimerSlackAwareBackoffIdleStrategy} works well.
 * <p>
 * <b>Two execution modes:</b>
 * <ul>
 *   <li>{@link #startOwnThread} — dedicated worker thread that calls {@link #tick} in a loop
 *       interleaved with the supplied {@code IdleStrategy}.</li>
 *   <li>{@link #start} + {@link #tick} called directly from any single thread (e.g.
 *       an IOWorker's select loop). The wheel uses {@link System#nanoTime()} catch-up, so
 *       irregular call cadence just means multiple ticks advance at once.</li>
 * </ul>
 * <b>Threading model.</b> {@link #schedule} is callable from any thread only when the timer was
 * built with {@code multiThreaded = true}; with {@code false} the submission ring buffer is SPSC
 * and a single producer thread must own every {@code schedule(...)} call. {@link #tick} must be
 * called from a single thread for the lifetime of the timer (the wheel arrays are not
 * synchronized). The lifecycle methods — {@link #start}, {@link #startOwnThread} and {@link #stop} — are
 * synchronized on the timer and so run one at a time; {@code schedule} and {@code tick} take no lock.
 * {@link #stop} must not be called from the worker/tick thread, as it joins that thread.
 */
@Slf4j
public final class WheelTimer {

    private static final int STATE_INIT = 0;
    private static final int STATE_STARTED = 1;
    private static final int STATE_STOPPED = 2;

    private static final TimerTaskExceptionHandler LOGGING_EXCEPTION_HANDLER =
            (task, exception) -> log.error("Timer task {} threw", task, exception);

    private static final Runnable NO_WAKEUP = () -> {
        // nothing to wake: either no worker thread, or the timer is stopped
    };

    /**
     * How long {@link #drainAndCancel()} keeps trying to empty the submission buffer. It is not waiting for
     * producers to stop — those are rejected by {@link #submit} — but for a producer caught mid-{@code offer}
     * to finish publishing, which is a store away.
     */
    private static final long DRAIN_TIMEOUT_NANOS = TimeUnit.MILLISECONDS.toNanos(100);

    @Getter
    private final long tickDurationNanos;
    @Getter
    private final int wheelSize;
    private final int mask;
    private final Bucket[] wheel;
    private final RingBuffer<TimeoutNode> submissions;
    private final Consumer<TimeoutNode> submissionDrainer = this::scheduleIntoWheel;
    private final Consumer<TimeoutNode> cancelDrainer = TimeoutNode::cancel;

    private volatile int state = STATE_INIT;
    private volatile TimerTaskExceptionHandler taskExceptionHandler = LOGGING_EXCEPTION_HANDLER;
    private volatile Thread worker;
    private long startTimeNanos;
    private long currentTick;
    private volatile Runnable notifyTaskScheduled = NO_WAKEUP;

    /**
     * @param tickDurationNanos   length of one tick in nanoseconds; must be positive
     * @param wheelSize           number of buckets; must be a positive power of two
     * @param submissionQueueSize capacity of the submission ring buffer; must be a positive power
     *                            of two. A full queue is what makes a schedule call come back
     *                            {@link Timeout#isRejected() rejected}
     * @param multiThreaded       {@code true} if more than one thread may call the
     *                            {@code schedule(...)} methods, which selects an MPSC submission
     *                            ring buffer instead of an SPSC one
     * @throws IllegalArgumentException if any of the above constraints is violated
     */
    public WheelTimer(long tickDurationNanos, int wheelSize, int submissionQueueSize, boolean multiThreaded) {
        if (tickDurationNanos <= 0L) {
            throw new IllegalArgumentException("tickDurationNanos must be > 0");
        }
        if (wheelSize <= 0 || Integer.bitCount(wheelSize) != 1) {
            throw new IllegalArgumentException("wheelSize must be a positive power of two");
        }
        if (submissionQueueSize <= 0 || Integer.bitCount(submissionQueueSize) != 1) {
            throw new IllegalArgumentException("submissionQueueSize must be a positive power of two");
        }
        this.tickDurationNanos = tickDurationNanos;
        this.wheelSize = wheelSize;
        this.mask = wheelSize - 1;
        this.wheel = new Bucket[wheelSize];
        for (int i = 0; i < wheelSize; i++) {
            this.wheel[i] = new Bucket();
        }
        this.submissions = RingBufferFactory.build(
                multiThreaded ? RingBufferFactory.AccessType.SINGLE_CONSUMER_MULTI_PRODUCER : RingBufferFactory.AccessType.SINGLE_CONSUMER_SINGLE_PRODUCER,
                submissionQueueSize);
    }

    /**
     * Schedules {@code task} to run after at least {@code delay} {@code unit}s have elapsed.
     * <p>
     * Returns a handle whose {@link Timeout#isRejected()} is {@code true} when the task never
     * reached the wheel — either the submission ring buffer was full at call time, or the timer
     * was not started. The task is then guaranteed to never fire; the caller can retry or surface
     * the rejection. We deliberately do not block here: this is the cross-thread fast path.
     *
     * @param task  the work to run when the deadline elapses
     * @param delay delay value; non-positive values fire on the next tick
     * @param unit  unit of {@code delay}
     * @return handle to cancel the task with and to observe its outcome through
     */
    public Timeout schedule(Runnable task, long delay, TimeUnit unit) {
        return schedule(task, null, delay, unit);
    }

    /**
     * Variant of {@link #schedule(Runnable, long, TimeUnit)} that attaches a per-task completion
     * callback. The callback is invoked from the tick/worker thread immediately after the task
     * runs, with:
     * <ul>
     *   <li>{@code (task, null)} on successful completion;</li>
     *   <li>{@code (task, exception)} when the task threw a checked or unchecked
     *       {@link Exception}.</li>
     * </ul>
     * The callback is invoked exactly once per firing, and <i>not</i> invoked at all if the task is
     * cancelled or if scheduling is rejected (full submission queue or timer not started — see
     * {@link Timeout#isRejected()}). An {@link Error} from the task is swallowed defensively so the worker
     * survives, and reported to the {@link TimerTaskExceptionHandler} rather than through this callback.
     * <p>
     * The callback runs on the tick/worker thread, so it must be cheap and non-blocking — the
     * same rules that apply to {@code task} itself. A {@code Throwable} thrown by the callback goes to the
     * {@link TimerTaskExceptionHandler}; it is never handed back to the callback as if the task had failed.
     * <p>
     * Pass {@code null} for {@code taskCallback} for the same behavior as the 3-arg overload.
     *
     * @param task         the work to run when the deadline elapses
     * @param taskCallback completion hook; may be {@code null}
     * @param delay        delay value; non-positive values fire on the next tick
     * @param unit         unit of {@code delay}
     * @return handle to cancel the task with and to observe its outcome through
     */
    public Timeout schedule(Runnable task, BiConsumer<Runnable, Exception> taskCallback, long delay, TimeUnit unit) {
        long deadlineNanos = System.nanoTime() + unit.toNanos(Math.max(0L, delay));
        return submit(new TimeoutNode(deadlineNanos, task, taskCallback));
    }

    /**
     * Replaces the handler notified when a task, or a task's completion callback, throws. The default logs
     * at error level, so a failure is never silent; supply your own to route failures into metrics or
     * alerting instead.
     * <p>
     * This is the <b>only</b> report of a failure for the
     * {@link TimerTaskOneArg}/{@link TimerTaskTwoArg}/{@link TimerTaskThreeArg} overloads, which carry no
     * per-task callback. Set it before {@link #start} — it is read on the tick thread.
     *
     * @param handler the handler to install; must not be {@code null}
     * @return this timer, for chaining
     */
    public WheelTimer setTaskExceptionHandler(TimerTaskExceptionHandler handler) {
        this.taskExceptionHandler = Objects.requireNonNull(handler, "handler");
        return this;
    }

    /**
     * Allocates a reusable {@link MutableTimeout} handle, callable from any thread. The
     * returned handle can be passed back into the {@code schedule(MutableTimeout, ...)}
     * overloads to avoid allocating a fresh timeout per call. See {@link MutableTimeout} for
     * the recycling contract.
     *
     * @return a fresh handle, ready to be passed to a {@code schedule(MutableTimeout, ...)} overload
     */
    public MutableTimeout newReusableTimeout() {
        return TimeoutNode.reusable();
    }

    /**
     * Reusable-handle variant of {@link #schedule(Runnable, long, TimeUnit)}. The handle
     * must come from {@link #newReusableTimeout()} and must be in a terminal state
     * (post-fire, rejected, or freshly allocated) — otherwise {@link IllegalStateException}
     * is thrown.
     *
     * @param reusable handle to recycle, obtained from {@link #newReusableTimeout()}
     * @param task     the work to run when the deadline elapses
     * @param delay    delay value; non-positive values fire on the next tick
     * @param unit     unit of {@code delay}
     * @return {@code reusable}, re-armed for this scheduling cycle
     * @throws IllegalStateException if the handle's previous use has not reached a terminal state
     */
    public Timeout schedule(MutableTimeout reusable, Runnable task, long delay, TimeUnit unit) {
        return schedule(reusable, task, null, delay, unit);
    }

    /**
     * Reusable-handle variant of {@link #schedule(Runnable, BiConsumer, long, TimeUnit)}.
     *
     * @param reusable     handle to recycle, obtained from {@link #newReusableTimeout()}
     * @param task         the work to run when the deadline elapses
     * @param taskCallback completion hook; may be {@code null}
     * @param delay        delay value; non-positive values fire on the next tick
     * @param unit         unit of {@code delay}
     * @return {@code reusable}, re-armed for this scheduling cycle
     * @throws IllegalStateException if the handle's previous use has not reached a terminal state
     */
    public Timeout schedule(MutableTimeout reusable, Runnable task, BiConsumer<Runnable, Exception> taskCallback,
                            long delay, TimeUnit unit) {
        TimeoutNode node = (TimeoutNode) reusable;
        long deadlineNanos = System.nanoTime() + unit.toNanos(Math.max(0L, delay));
        return submit(node.setup(deadlineNanos, task, taskCallback));
    }

    /**
     * Schedules a one-argument task. The task can be held in a static field and the argument
     * passed separately, which avoids allocating a capturing lambda per call.
     *
     * @param task  the work to run when the deadline elapses
     * @param arg1  the argument handed to {@code task} on expiry
     * @param delay delay value; non-positive values fire on the next tick
     * @param unit  unit of {@code delay}
     * @param <A>   the argument type
     * @return handle to cancel the task with and to observe its outcome through
     * @see TimerTaskOneArg
     */
    public <A> Timeout schedule(TimerTaskOneArg<A> task, A arg1, long delay, TimeUnit unit) {
        long deadlineNanos = System.nanoTime() + unit.toNanos(Math.max(0L, delay));
        return submit(new TimeoutNode(deadlineNanos, task, arg1));
    }

    /**
     * Reusable-handle variant of {@link #schedule(TimerTaskOneArg, Object, long, TimeUnit)}.
     *
     * @param reusable handle to recycle, obtained from {@link #newReusableTimeout()}
     * @param task     the work to run when the deadline elapses
     * @param arg1     the argument handed to {@code task} on expiry
     * @param delay    delay value; non-positive values fire on the next tick
     * @param unit     unit of {@code delay}
     * @param <A>      the argument type
     * @return {@code reusable}, re-armed for this scheduling cycle
     * @throws IllegalStateException if the handle's previous use has not reached a terminal state
     */
    public <A> Timeout schedule(MutableTimeout reusable, TimerTaskOneArg<A> task, A arg1,
                                long delay, TimeUnit unit) {
        long deadlineNanos = System.nanoTime() + unit.toNanos(Math.max(0L, delay));
        return submit(((TimeoutNode) reusable).setup(deadlineNanos, task, arg1));
    }

    /**
     * Schedules a two-argument task. The task can be held in a static field and the arguments
     * passed separately, which avoids allocating a capturing lambda per call.
     *
     * @param task  the work to run when the deadline elapses
     * @param arg1  the first argument handed to {@code task} on expiry
     * @param arg2  the second argument handed to {@code task} on expiry
     * @param delay delay value; non-positive values fire on the next tick
     * @param unit  unit of {@code delay}
     * @param <A>   the first argument type
     * @param <B>   the second argument type
     * @return handle to cancel the task with and to observe its outcome through
     * @see TimerTaskTwoArg
     */
    public <A, B> Timeout schedule(TimerTaskTwoArg<A, B> task, A arg1, B arg2, long delay, TimeUnit unit) {
        long deadlineNanos = System.nanoTime() + unit.toNanos(Math.max(0L, delay));
        return submit(new TimeoutNode(deadlineNanos, task, arg1, arg2));
    }

    /**
     * Reusable-handle variant of {@link #schedule(TimerTaskTwoArg, Object, Object, long, TimeUnit)}.
     *
     * @param reusable handle to recycle, obtained from {@link #newReusableTimeout()}
     * @param task     the work to run when the deadline elapses
     * @param arg1     the first argument handed to {@code task} on expiry
     * @param arg2     the second argument handed to {@code task} on expiry
     * @param delay    delay value; non-positive values fire on the next tick
     * @param unit     unit of {@code delay}
     * @param <A>      the first argument type
     * @param <B>      the second argument type
     * @return {@code reusable}, re-armed for this scheduling cycle
     * @throws IllegalStateException if the handle's previous use has not reached a terminal state
     */
    public <A, B> Timeout schedule(MutableTimeout reusable, TimerTaskTwoArg<A, B> task, A arg1, B arg2,
                                   long delay, TimeUnit unit) {
        long deadlineNanos = System.nanoTime() + unit.toNanos(Math.max(0L, delay));
        return submit(((TimeoutNode) reusable).setup(deadlineNanos, task, arg1, arg2));
    }

    /**
     * Schedules a three-argument task. The task can be held in a static field and the arguments
     * passed separately, which avoids allocating a capturing lambda per call.
     *
     * @param task  the work to run when the deadline elapses
     * @param arg1  the first argument handed to {@code task} on expiry
     * @param arg2  the second argument handed to {@code task} on expiry
     * @param arg3  the third argument handed to {@code task} on expiry
     * @param delay delay value; non-positive values fire on the next tick
     * @param unit  unit of {@code delay}
     * @param <A>   the first argument type
     * @param <B>   the second argument type
     * @param <C>   the third argument type
     * @return handle to cancel the task with and to observe its outcome through
     * @see TimerTaskThreeArg
     */
    public <A, B, C> Timeout schedule(TimerTaskThreeArg<A, B, C> task, A arg1, B arg2, C arg3,
                                      long delay, TimeUnit unit) {
        long deadlineNanos = System.nanoTime() + unit.toNanos(Math.max(0L, delay));
        return submit(new TimeoutNode(deadlineNanos, task, arg1, arg2, arg3));
    }

    /**
     * Reusable-handle variant of {@link #schedule(TimerTaskThreeArg, Object, Object, Object, long, TimeUnit)}.
     *
     * @param reusable handle to recycle, obtained from {@link #newReusableTimeout()}
     * @param task     the work to run when the deadline elapses
     * @param arg1     the first argument handed to {@code task} on expiry
     * @param arg2     the second argument handed to {@code task} on expiry
     * @param arg3     the third argument handed to {@code task} on expiry
     * @param delay    delay value; non-positive values fire on the next tick
     * @param unit     unit of {@code delay}
     * @param <A>      the first argument type
     * @param <B>      the second argument type
     * @param <C>      the third argument type
     * @return {@code reusable}, re-armed for this scheduling cycle
     * @throws IllegalStateException if the handle's previous use has not reached a terminal state
     */
    public <A, B, C> Timeout schedule(MutableTimeout reusable, TimerTaskThreeArg<A, B, C> task,
                                      A arg1, B arg2, C arg3, long delay, TimeUnit unit) {
        long deadlineNanos = System.nanoTime() + unit.toNanos(Math.max(0L, delay));
        return submit(((TimeoutNode) reusable).setup(deadlineNanos, task, arg1, arg2, arg3));
    }

    private Timeout submit(TimeoutNode node) {
        if (state != STATE_STARTED) {
            node.markRejected();
            return node;
        }
        if (!submissions.offer(node)) {
            node.markRejected();
            return node;
        }
        // Re-check after publishing. A stop() racing with the offer above may already have drained the
        // submission buffer past this node, which would leave it sitting there in a non-terminal state for
        // ever — the caller's Timeout would report neither expired, nor cancelled, nor rejected. Whichever
        // of the two runs second now terminates the node, at the cost of one more volatile read here.
        if (state != STATE_STARTED) {
            node.markRejected();
            return node;
        }
        notifyTaskScheduled.run();
        return node;
    }

    /**
     * Starts a dedicated worker thread that loops {@link #tick} interleaved with
     * {@code idleStrategy.idle()}. Pick the idle strategy to match the latency/CPU profile.
     *
     * <p><b>Important:</b> the supplied strategy's {@code idle()} must not block longer than
     * {@code tickDurationNanos}; otherwise the wheel cannot advance between submissions and
     * scheduled tasks never fire. Safe choices include {@link org.lolaf.ringos.idling.BusySpinIdleStrategy},
     * {@link org.lolaf.ringos.idling.BackoffIdleStrategy}, and
     * {@link org.lolaf.ringos.idling.TimedWaitNotifyIdleStrategy}. The plain
     * {@link WaitNotifyIdleStrategy} blocks indefinitely on {@code Object.wait()} and is rejected
     * here to surface the gotcha at startup instead of via silent deadlock.
     *
     * @param idleStrategy  how the worker thread idles between ticks
     * @param threadFactory factory used to create the worker thread
     * @throws IllegalArgumentException if {@code idleStrategy} is a {@link WaitNotifyIdleStrategy};
     *                                  use {@link org.lolaf.ringos.idling.TimedWaitNotifyIdleStrategy} instead
     */
    public synchronized void startOwnThread(IdleStrategy idleStrategy, ThreadFactory threadFactory) {
        Objects.requireNonNull(idleStrategy, "idleStrategy");
        Objects.requireNonNull(threadFactory, "threadFactory");
        if (idleStrategy instanceof WaitNotifyIdleStrategy) {
            throw new IllegalArgumentException(
                    "WaitNotifyIdleStrategy.idle() blocks indefinitely on Object.wait() and would prevent " +
                            "the wheel from advancing between submissions. Use BackoffIdleStrategy, " +
                            "BusySpinIdleStrategy, or TimedWaitNotifyIdleStrategy(maxWait) instead.");
        }
        if (state == STATE_STARTED) {
            return;
        }
        start();
        Thread t = threadFactory.newThread(() -> runOwnThread(idleStrategy));
        if (t.getUncaughtExceptionHandler() == null) {
            t.setUncaughtExceptionHandler((th, e) -> log.error("CRITICAL: uncaught exception in WheelTimer worker {}", th.getName(), e));
        }
        worker = t;
        notifyTaskScheduled = idleStrategy::wakeup;
        t.start();
    }

    /**
     * Marks the timer started without launching a worker thread. The caller is responsible
     * for invoking {@link #tick} from a single thread. Also resets the wheel's time origin, so
     * a timer restarted after {@link #stop} begins counting from zero again.
     *
     * @return this timer, for chaining
     */
    public synchronized WheelTimer start() {
        if (state == STATE_STARTED) {
            return this;
        }
        startTimeNanos = System.nanoTime();
        currentTick = 0L;
        // Volatile write last, so the tick thread that reads STATE_STARTED also sees the time origin above.
        state = STATE_STARTED;
        return this;
    }

    /**
     * @return {@code true} between {@link #start} (or {@link #startOwnThread}) and {@link #stop};
     * while this is {@code false} every {@code schedule(...)} call is rejected and {@link #tick}
     * is a no-op
     */
    public boolean isStarted() {
        return state == STATE_STARTED;
    }

    /**
     * Stops the timer. Any pending or in-flight task is cancelled, and every subsequent
     * {@code schedule(...)} is {@link Timeout#isRejected() rejected}.
     * <p>
     * In own-thread mode the worker is interrupted and joined, bounded by {@code stopTimeout}, and the
     * worker cancels the outstanding tasks on its own way out — the wheel arrays and the submission ring
     * buffer are single-consumer, so they may only be touched from the tick thread. If the worker overruns
     * {@code stopTimeout} this returns anyway, having logged an error: its tasks stay scheduled until it
     * does exit, which is the price of not racing a thread that still owns the wheel.
     * <p>
     * In tick-driven mode the caller must stop invoking {@link #tick} before calling this, for the same
     * reason; the cancellation then happens on the calling thread.
     * <p>
     * Idempotent, and safe to call concurrently: the method is synchronized, so a second caller waits for
     * the first to finish and then returns, rather than running a second shutdown alongside it — two of them
     * would drain the wheel and the submission buffer concurrently. Must not be called from the worker/tick
     * thread — it joins that thread.
     *
     * @param stopTimeout how long to wait for the worker thread to terminate; an overrun is logged as an
     *                    error rather than thrown, and values below 10 ms are raised to 10 ms
     * @return this timer, for chaining
     */
    public synchronized WheelTimer stop(Duration stopTimeout) {
        Objects.requireNonNull(stopTimeout, "stopTimeout");
        if (state == STATE_STOPPED) {
            return this;
        }
        state = STATE_STOPPED;
        notifyTaskScheduled = NO_WAKEUP;
        Thread stopping = worker;
        if (stopping != null) {
            stopping.interrupt();
            try {
                stopping.join(Math.max(10L, stopTimeout.toMillis()));
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            if (stopping.isAlive()) {
                // The one case where we must not clean up: the worker still owns the wheel and the
                // submission buffer, and draining them from here is exactly the concurrent mutation this
                // guards against. It cancels its own outstanding work when it finally exits.
                log.error("WheelTimer worker {} did not terminate within {}; it still owns the wheel, so its"
                        + " pending tasks stay scheduled until it exits", stopping.getName(), stopTimeout);
                return this;
            }
            worker = null;
        }
        // Nothing else is consuming the wheel now — either there was never a worker (tick-driven mode, where
        // the caller guarantees tick() has stopped) or it has exited. Draining twice is harmless: the worker
        // already did this on its way out, and both the buckets and the submission buffer end up empty.
        drainAndCancel();
        return this;
    }

    /**
     * Advances the wheel up to the current wall-clock tick, draining any pending submissions
     * first. Safe to call before {@link #start} or after {@link #stop}; returns {@code 0} without
     * touching the wheel in that case, so a select loop can keep calling it across the timer's
     * whole lifecycle. Must be called from a single thread for the lifetime of the timer.
     *
     * @return number of tasks that fired during this call (excludes cancelled tasks and tasks
     * drained from the submission queue but not yet due). Useful for work-count based
     * idle strategies in tick-driven mode.
     */
    public int tick() {
        if (state != STATE_STARTED) {
            // Without this guard startTimeNanos is still 0 before the first start(), so the
            // catch-up loop below would try to walk every tick since the nanoTime() epoch.
            return 0;
        }
        // Drain submissions first so a task scheduled inside the same tick still has a
        // chance to fire this round if it was already due.
        submissions.drain(submissionDrainer);
        long elapsed = System.nanoTime() - startTimeNanos;
        long targetTick = elapsed / tickDurationNanos;
        // Read the volatile once per tick rather than once per bucket walked.
        TimerTaskExceptionHandler exceptionHandler = taskExceptionHandler;
        int fired = 0;
        while (currentTick < targetTick) {
            currentTick++;
            Bucket bucket = wheel[(int) ((currentTick - 1) & mask)];
            fired += bucket.expire(exceptionHandler);
        }
        return fired;
    }

    private void scheduleIntoWheel(TimeoutNode node) {
        if (node.isCancelled()) {
            return;
        }
        long delayTicks = (node.getDeadlineNanos() - startTimeNanos) / tickDurationNanos;
        if (delayTicks <= currentTick) {
            // Past or current tick: schedule one tick ahead so this fires on the next
            // expire walk rather than waiting a full wheel rotation.
            delayTicks = currentTick + 1;
        }

        node.remainingRounds = (delayTicks - currentTick - 1) / wheelSize;
        int bucketIdx = (int) ((delayTicks - 1) & mask);
        wheel[bucketIdx].add(node);
    }

    private void runOwnThread(IdleStrategy idleStrategy) {
        log.info("WheelTimer worker {} started (tick={} ns, wheelSize={})",
                Thread.currentThread().getName(), tickDurationNanos, wheelSize);
        try {
            while (state == STATE_STARTED) {
                idleStrategy.idle(tick());
            }
        } catch (Throwable t) {
            // tick() itself failed, so nothing will advance the wheel any more. Mark the timer stopped
            // rather than leaving callers queueing into a wheel no one walks; the thread's uncaught
            // handler still reports the failure. Deliberately not synchronized: stop() holds this timer's
            // monitor while it joins us, and Thread.join does not release it, so taking the lock here would
            // wedge us until stop() gave up and wrongly reported an overrun.
            state = STATE_STOPPED;
            throw t;
        } finally {
            // The drain belongs on this thread: the wheel arrays and the submission ring buffer are
            // single-consumer, and stop() deliberately leaves them alone while this thread is alive. The
            // identity check keeps a worker that outlived its join from draining a restarted timer's wheel.
            if (Thread.currentThread() == worker) {
                drainAndCancel();
            }
            log.info("WheelTimer worker {} stopped", Thread.currentThread().getName());
        }
    }

    private void drainAndCancel() {
        // One pass is not enough. A producer that has claimed a slot but not yet published it makes the
        // buffer look empty at that slot, so the drain stops there — and anything already published *behind*
        // it stays in the buffer for ever: never run, and never reported to whoever scheduled it, whose
        // Timeout then reports neither expired, nor cancelled, nor rejected. Producers arriving from here on
        // are rejected by submit(), so what is left to wait for is only an offer already in flight.
        long deadline = System.nanoTime() + DRAIN_TIMEOUT_NANOS;
        do {
            submissions.drain(cancelDrainer);
            if (submissions.isEmpty()) {
                break;
            }
            Thread.onSpinWait();
        } while (System.nanoTime() < deadline);
        if (submissions.isNotEmpty()) {
            log.warn("WheelTimer submission buffer still reports entries after {} ms of draining; timeouts"
                    + " left there stay uncancelled", TimeUnit.NANOSECONDS.toMillis(DRAIN_TIMEOUT_NANOS));
        }
        for (Bucket bucket : wheel) {
            bucket.drain();
        }
    }
}
