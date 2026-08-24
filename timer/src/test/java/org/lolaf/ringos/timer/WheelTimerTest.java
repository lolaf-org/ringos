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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.lolaf.ringos.idling.BackoffIdleStrategy;
import org.lolaf.ringos.idling.IdleStrategy;
import org.lolaf.ringos.idling.TimedWaitNotifyIdleStrategy;
import org.lolaf.ringos.idling.WaitNotifyIdleStrategy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WheelTimerTest {

    private WheelTimer timer;

    @AfterEach
    void tearDown() {
        if (timer != null) {
            timer.stop(Duration.ofSeconds(2));
        }
    }

    private IdleStrategy backoff() {
        return new BackoffIdleStrategy(10, 10,
                TimeUnit.MICROSECONDS.toNanos(10), TimeUnit.MICROSECONDS.toNanos(500));
    }

    private ThreadFactory threadFactory() {
        return r -> {
            Thread t = new Thread(r, "wheel-timer-test");
            t.setDaemon(true);
            return t;
        };
    }

    private WheelTimer newTimer(long tickNanos, int wheelSize) {
        return new WheelTimer(tickNanos, wheelSize, 1024, false);
    }

    @Test
    void rejectsInvalidConstructorArgs() {
        assertThatThrownBy(() -> new WheelTimer(0, 16, 16, false)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new WheelTimer(1000, 15, 16, false)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new WheelTimer(1000, 16, 15, false)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void firesScheduledTaskAfterDelay() throws Exception {
        timer = newTimer(TimeUnit.MILLISECONDS.toNanos(1), 512);
        timer.startOwnThread(backoff(), threadFactory());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicLong firedAt = new AtomicLong();
        long scheduledAt = System.nanoTime();
        timer.schedule(() -> {
            firedAt.set(System.nanoTime());
            latch.countDown();
        }, 50, TimeUnit.MILLISECONDS);

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        long elapsedNanos = firedAt.get() - scheduledAt;
        assertThat(elapsedNanos).isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(45));
    }

    @Test
    void cancelledTaskDoesNotFire() throws Exception {
        timer = newTimer(TimeUnit.MILLISECONDS.toNanos(1), 512);
        timer.startOwnThread(backoff(), threadFactory());

        AtomicInteger fired = new AtomicInteger();
        Timeout to = timer.schedule(fired::incrementAndGet, 100, TimeUnit.MILLISECONDS);
        assertThat(to.cancel()).isTrue();
        assertThat(to.isCancelled()).isTrue();

        Thread.sleep(200);
        assertThat(fired.get()).isZero();
        // second cancel is a no-op
        assertThat(to.cancel()).isFalse();
    }

    @Test
    void firesPastOneWheelRotation() throws Exception {
        // 1ms tick, 16 buckets -> one rotation = 16ms; schedule at 60ms (~3.75 rotations)
        timer = newTimer(TimeUnit.MILLISECONDS.toNanos(1), 16);
        timer.startOwnThread(backoff(), threadFactory());

        CountDownLatch latch = new CountDownLatch(1);
        long scheduledAt = System.nanoTime();
        timer.schedule(latch::countDown, 60, TimeUnit.MILLISECONDS);

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        long elapsedNanos = System.nanoTime() - scheduledAt;
        assertThat(elapsedNanos).isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(55));
    }

    @Test
    void handlesConcurrentSubmissions() throws Exception {
        timer = new WheelTimer(TimeUnit.MILLISECONDS.toNanos(1), 512, 1024, true);
        timer.startOwnThread(backoff(), threadFactory());

        int producers = 8;
        int perProducer = 64;
        int total = producers * perProducer;
        CountDownLatch fired = new CountDownLatch(total);
        AtomicInteger rejected = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(producers);
        try {
            CountDownLatch start = new CountDownLatch(1);
            for (int p = 0; p < producers; p++) {
                pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < perProducer; i++) {
                        Timeout to = timer.schedule(fired::countDown, 30, TimeUnit.MILLISECONDS);
                        if (to.isRejected()) {
                            rejected.incrementAndGet();
                            fired.countDown();
                        }
                    }
                    return null;
                });
            }
            start.countDown();
            assertThat(fired.await(3, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }
        assertThat(rejected.get()).isZero(); // 512 capacity > 512 submitted
    }

    @Test
    void scheduleReturnsRejectedWhenQueueFull() {
        // Tick-driven mode and never call tick() -> submissions accumulate until full
        timer = new WheelTimer(TimeUnit.MILLISECONDS.toNanos(1), 16, 8, false);
        timer.start();

        AtomicReferenceArray<Timeout> outcomes = new AtomicReferenceArray<>(20);
        for (int i = 0; i < 20; i++) {
            outcomes.set(i, timer.schedule(() -> {
            }, 100, TimeUnit.MILLISECONDS));
        }
        int rejected = 0;
        for (int i = 0; i < 20; i++) {
            if (outcomes.get(i).isRejected()) {
                rejected++;
            }
        }
        assertThat(rejected).isGreaterThan(0);
        assertThat(rejected).isLessThanOrEqualTo(20 - 8);
    }

    @Test
    void tickDrivenModeFiresWhenTickCalled() throws Exception {
        timer = new WheelTimer(TimeUnit.MILLISECONDS.toNanos(1), 16, 64, false);
        timer.start();

        AtomicInteger fired = new AtomicInteger();
        timer.schedule(fired::incrementAndGet, 5, TimeUnit.MILLISECONDS);
        Thread.sleep(20);
        // before any tick(), nothing has fired
        assertThat(fired.get()).isZero();

        timer.tick();
        assertThat(fired.get()).isEqualTo(1);
    }

    @Test
    void tickBeforeStartAndAfterStopIsANoOp() {
        timer = new WheelTimer(TimeUnit.MILLISECONDS.toNanos(1), 16, 64, false);

        // Before start() the wheel has no time origin; tick() must not try to catch up from it.
        assertThat(timer.tick()).isZero();

        timer.start();
        AtomicInteger fired = new AtomicInteger();
        timer.schedule(fired::incrementAndGet, 1, TimeUnit.MILLISECONDS);

        timer.stop(Duration.ofSeconds(1));
        assertThat(timer.tick()).isZero();
        assertThat(fired.get()).isZero();
        timer = null; // already stopped
    }

    @Test
    void scheduleAfterStopIsRejected() {
        timer = newTimer(TimeUnit.MILLISECONDS.toNanos(1), 16);
        timer.startOwnThread(backoff(), threadFactory());
        timer.stop(Duration.ofSeconds(1));

        Timeout to = timer.schedule(() -> {
        }, 10, TimeUnit.MILLISECONDS);
        assertThat(to.isRejected()).isTrue();
        timer = null; // already stopped
    }

    @Test
    void reusableTimeoutFiresMultipleTimes() throws Exception {
        timer = newTimer(TimeUnit.MILLISECONDS.toNanos(1), 512);
        timer.startOwnThread(backoff(), threadFactory());

        MutableTimeout reusable = timer.newReusableTimeout();
        AtomicInteger fired = new AtomicInteger();

        for (int i = 0; i < 5; i++) {
            CountDownLatch latch = new CountDownLatch(1);
            Timeout t = timer.schedule(reusable, () -> {
                fired.incrementAndGet();
                latch.countDown();
            }, 10, TimeUnit.MILLISECONDS);
            assertThat(t).isSameAs(reusable);
            assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
            awaitTrue(reusable::isExpired);
        }
        assertThat(fired.get()).isEqualTo(5);
    }

    @Test
    void reusableTimeoutSchedulableAfterReject() {
        // Schedule on a stopped timer -> guaranteed REJECTED. Verify the handle can be
        // recycled afterward.
        timer = newTimer(TimeUnit.MILLISECONDS.toNanos(1), 16);
        timer.start();
        timer.stop(Duration.ofSeconds(1));

        MutableTimeout reusable = timer.newReusableTimeout();
        Timeout first = timer.schedule(reusable, () -> {
        }, 100, TimeUnit.MILLISECONDS);
        assertThat(first.isRejected()).isTrue();

        // recycling after reject is allowed
        Timeout second = timer.schedule(reusable, () -> {
        }, 100, TimeUnit.MILLISECONDS);
        assertThat(second.isRejected()).isTrue();
        timer = null; // already stopped
    }

    @Test
    void reusableTimeoutInFlightCannotBeRecycled() {
        timer = new WheelTimer(TimeUnit.MILLISECONDS.toNanos(1), 16, 64, false);
        timer.start();

        MutableTimeout reusable = timer.newReusableTimeout();
        timer.schedule(reusable, () -> {
        }, 500, TimeUnit.MILLISECONDS); // never ticks, stays scheduled

        assertThatThrownBy(() -> timer.schedule(reusable, () -> {
        }, 10, TimeUnit.MILLISECONDS))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void reusableTimeoutAfterCancelCannotBeRecycled() {
        timer = new WheelTimer(TimeUnit.MILLISECONDS.toNanos(1), 16, 64, false);
        timer.start();

        MutableTimeout reusable = timer.newReusableTimeout();
        Timeout t = timer.schedule(reusable, () -> {
        }, 500, TimeUnit.MILLISECONDS);
        assertThat(t.cancel()).isTrue();

        assertThatThrownBy(() -> timer.schedule(reusable, () -> {
        }, 10, TimeUnit.MILLISECONDS))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void stopCancelsPendingTasks() throws Exception {
        timer = new WheelTimer(TimeUnit.MILLISECONDS.toNanos(1), 16, 64, false);
        timer.start();

        AtomicInteger fired = new AtomicInteger();
        Timeout to = timer.schedule(fired::incrementAndGet, 100, TimeUnit.MILLISECONDS);

        timer.stop(Duration.ofSeconds(1));
        Thread.sleep(150);
        assertThat(fired.get()).isZero();
        assertThat(to.isCancelled()).isTrue();
        timer = null;
    }

    @Test
    void firesScheduledTaskWithTimedWaitNotifyIdleStrategy() throws Exception {
        long tickNanos = TimeUnit.MILLISECONDS.toNanos(1);
        timer = newTimer(tickNanos, 512);
        // Bounded wait at one tick — the wheel must still advance even when no submissions arrive
        // for a while; this is the property plain WaitNotifyIdleStrategy doesn't satisfy.
        timer.startOwnThread(new TimedWaitNotifyIdleStrategy(Duration.ofNanos(tickNanos)), threadFactory());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicLong firedAt = new AtomicLong();
        long scheduledAt = System.nanoTime();
        timer.schedule(() -> {
            firedAt.set(System.nanoTime());
            latch.countDown();
        }, 50, TimeUnit.MILLISECONDS);

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(firedAt.get() - scheduledAt);
        // Generous bounds — CI scheduling jitter is real; we only care that the wheel did
        // advance on its own (the bug under plain WaitNotifyIdleStrategy was that it never did).
        assertThat(elapsedMs).isBetween(40L, 500L);
    }

    @Test
    void firesManyConsecutiveTasksWithTimedWaitNotifyIdleStrategy() throws Exception {
        long tickNanos = TimeUnit.MILLISECONDS.toNanos(1);
        timer = newTimer(tickNanos, 512);
        timer.startOwnThread(new TimedWaitNotifyIdleStrategy(Duration.ofNanos(tickNanos)), threadFactory());

        // Schedule 20 tasks spaced 5ms apart; if the wheel only advances on submission wakeup,
        // bursts after a quiet gap would drop and the latch wouldn't reach zero.
        int taskCount = 20;
        CountDownLatch latch = new CountDownLatch(taskCount);
        for (int i = 0; i < taskCount; i++) {
            timer.schedule(latch::countDown, 5L * (i + 1), TimeUnit.MILLISECONDS);
        }
        assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void rejectsPlainWaitNotifyIdleStrategyInStartOwnThread() {
        timer = newTimer(TimeUnit.MILLISECONDS.toNanos(1), 16);
        assertThatThrownBy(() -> timer.startOwnThread(new WaitNotifyIdleStrategy(), threadFactory()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("WaitNotifyIdleStrategy")
                .hasMessageContaining("TimedWaitNotifyIdleStrategy");
    }

    @Test
    void firesOneArgTask() throws Exception {
        timer = newTimer(TimeUnit.MILLISECONDS.toNanos(1), 512);
        timer.startOwnThread(backoff(), threadFactory());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> captured = new AtomicReference<>();
        TimerTaskOneArg<String> task = arg -> {
            captured.set(arg);
            latch.countDown();
        };

        timer.schedule(task, "hello", 10, TimeUnit.MILLISECONDS);

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(captured.get()).isEqualTo("hello");
    }

    @Test
    void firesTwoArgTask() throws Exception {
        timer = newTimer(TimeUnit.MILLISECONDS.toNanos(1), 512);
        timer.startOwnThread(backoff(), threadFactory());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> capturedA = new AtomicReference<>();
        AtomicInteger capturedB = new AtomicInteger();
        TimerTaskTwoArg<String, Integer> task = (a, b) -> {
            capturedA.set(a);
            capturedB.set(b);
            latch.countDown();
        };

        timer.schedule(task, "world", 42, 10, TimeUnit.MILLISECONDS);

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(capturedA.get()).isEqualTo("world");
        assertThat(capturedB.get()).isEqualTo(42);
    }

    @Test
    void reusableTimeoutWithOneArgTask() throws Exception {
        timer = newTimer(TimeUnit.MILLISECONDS.toNanos(1), 512);
        timer.startOwnThread(backoff(), threadFactory());

        MutableTimeout reusable = timer.newReusableTimeout();
        AtomicInteger sum = new AtomicInteger();
        TimerTaskOneArg<Integer> task = sum::addAndGet;

        for (int i = 1; i <= 5; i++) {
            CountDownLatch latch = new CountDownLatch(1);
            TimerTaskOneArg<Integer> taskWithLatch = arg -> {
                task.execute(arg);
                latch.countDown();
            };
            timer.schedule(reusable, taskWithLatch, i, 10, TimeUnit.MILLISECONDS);
            assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
            awaitTrue(reusable::isExpired);
        }
        assertThat(sum.get()).isEqualTo(15);
    }

    @Test
    void firesThreeArgTask() throws Exception {
        timer = newTimer(TimeUnit.MILLISECONDS.toNanos(1), 512);
        timer.startOwnThread(backoff(), threadFactory());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> capturedA = new AtomicReference<>();
        AtomicInteger capturedB = new AtomicInteger();
        AtomicLong capturedC = new AtomicLong();
        TimerTaskThreeArg<String, Integer, Long> task = (a, b, c) -> {
            capturedA.set(a);
            capturedB.set(b);
            capturedC.set(c);
            latch.countDown();
        };

        timer.schedule(task, "abc", 7, 99L, 10, TimeUnit.MILLISECONDS);

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(capturedA.get()).isEqualTo("abc");
        assertThat(capturedB.get()).isEqualTo(7);
        assertThat(capturedC.get()).isEqualTo(99L);
    }

    @Test
    void reusableTimeoutWithTwoArgTask() throws Exception {
        timer = newTimer(TimeUnit.MILLISECONDS.toNanos(1), 512);
        timer.startOwnThread(backoff(), threadFactory());

        MutableTimeout reusable = timer.newReusableTimeout();
        AtomicLong result = new AtomicLong();
        TimerTaskTwoArg<Integer, Integer> multiply = (a, b) -> result.set((long) a * b);

        for (int i = 1; i <= 3; i++) {
            CountDownLatch latch = new CountDownLatch(1);
            int val = i;
            TimerTaskTwoArg<Integer, Integer> taskWithLatch = (a, b) -> {
                multiply.execute(a, b);
                latch.countDown();
            };
            timer.schedule(reusable, taskWithLatch, val, val + 10, 10, TimeUnit.MILLISECONDS);
            assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
            awaitTrue(reusable::isExpired);
            assertThat(result.get()).isEqualTo((long) val * (val + 10));
        }
    }

    @Test
    void reusableTimeoutWithThreeArgTask() throws Exception {
        timer = newTimer(TimeUnit.MILLISECONDS.toNanos(1), 512);
        timer.startOwnThread(backoff(), threadFactory());

        MutableTimeout reusable = timer.newReusableTimeout();
        AtomicReference<String> lastResult = new AtomicReference<>();

        for (int i = 1; i <= 3; i++) {
            CountDownLatch latch = new CountDownLatch(1);
            TimerTaskThreeArg<String, Integer, String> task = (prefix, num, suffix) -> {
                lastResult.set(prefix + num + suffix);
                latch.countDown();
            };
            timer.schedule(reusable, task, "v", i, "!", 10, TimeUnit.MILLISECONDS);
            assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
            awaitTrue(reusable::isExpired);
            assertThat(lastResult.get()).isEqualTo("v" + i + "!");
        }
    }

    // ---------------------------------------------------------------------------------------------------
    // Task failure reporting. Before TimerTaskExceptionHandler existed, an exception from any of the
    // arity overloads was caught by the bucket walk and dropped on the floor with nothing logged.
    // ---------------------------------------------------------------------------------------------------

    /**
     * Drives a tick-driven timer until {@code done} holds, or fails the test.
     */
    private void pumpUntil(BooleanSupplier done) throws Exception {
        for (int i = 0; i < 200 && !done.getAsBoolean(); i++) {
            timer.tick();
            Thread.sleep(2);
        }
        assertThat(done.getAsBoolean()).as("condition never reached after pumping ticks").isTrue();
    }

    private WheelTimer newTickDrivenTimer() {
        WheelTimer t = new WheelTimer(TimeUnit.MILLISECONDS.toNanos(1), 16, 64, false);
        t.start();
        return t;
    }

    @Test
    void reportsExceptionFromRunnableTaskToTheHandlerWhenThereIsNoCallback() throws Exception {
        timer = newTickDrivenTimer();
        AtomicReference<Object> failedTask = new AtomicReference<>();
        AtomicReference<Throwable> reported = new AtomicReference<>();
        timer.setTaskExceptionHandler((task, e) -> {
            failedTask.set(task);
            reported.set(e);
        });

        Runnable boom = () -> {
            throw new IllegalStateException("boom");
        };
        timer.schedule(boom, 1, TimeUnit.MILLISECONDS);

        pumpUntil(() -> reported.get() != null);
        assertThat(reported.get()).isInstanceOf(IllegalStateException.class).hasMessage("boom");
        assertThat(failedTask.get()).isSameAs(boom);
    }

    @Test
    void reportsExceptionFromOneArgTask() throws Exception {
        timer = newTickDrivenTimer();
        AtomicReference<Throwable> reported = new AtomicReference<>();
        timer.setTaskExceptionHandler((task, e) -> reported.set(e));

        TimerTaskOneArg<String> boom = arg -> {
            throw new IllegalStateException("one-arg " + arg);
        };
        timer.schedule(boom, "a", 1, TimeUnit.MILLISECONDS);

        pumpUntil(() -> reported.get() != null);
        assertThat(reported.get()).hasMessage("one-arg a");
    }

    @Test
    void reportsExceptionFromTwoArgTask() throws Exception {
        timer = newTickDrivenTimer();
        AtomicReference<Throwable> reported = new AtomicReference<>();
        timer.setTaskExceptionHandler((task, e) -> reported.set(e));

        TimerTaskTwoArg<String, String> boom = (a, b) -> {
            throw new IllegalStateException("two-arg " + a + b);
        };
        timer.schedule(boom, "a", "b", 1, TimeUnit.MILLISECONDS);

        pumpUntil(() -> reported.get() != null);
        assertThat(reported.get()).hasMessage("two-arg ab");
    }

    @Test
    void reportsExceptionFromThreeArgTask() throws Exception {
        timer = newTickDrivenTimer();
        AtomicReference<Throwable> reported = new AtomicReference<>();
        timer.setTaskExceptionHandler((task, e) -> reported.set(e));

        TimerTaskThreeArg<String, String, String> boom = (a, b, c) -> {
            throw new IllegalStateException("three-arg " + a + b + c);
        };
        timer.schedule(boom, "a", "b", "c", 1, TimeUnit.MILLISECONDS);

        pumpUntil(() -> reported.get() != null);
        assertThat(reported.get()).hasMessage("three-arg abc");
    }

    @Test
    void reportsErrorThrownByTaskAndKeepsTheWheelRunning() throws Exception {
        timer = newTickDrivenTimer();
        AtomicReference<Throwable> reported = new AtomicReference<>();
        timer.setTaskExceptionHandler((task, e) -> reported.set(e));

        timer.schedule(() -> {
            throw new StackOverflowError("deep");
        }, 1, TimeUnit.MILLISECONDS);
        pumpUntil(() -> reported.get() != null);
        assertThat(reported.get()).isInstanceOf(StackOverflowError.class);

        // the wheel survived and still fires
        AtomicInteger later = new AtomicInteger();
        timer.schedule(later::incrementAndGet, 1, TimeUnit.MILLISECONDS);
        pumpUntil(() -> later.get() == 1);
    }

    @Test
    void aTaskThatFailsStillReachesItsCompletionCallbackWithTheException() throws Exception {
        timer = newTickDrivenTimer();
        AtomicReference<Exception> viaCallback = new AtomicReference<>();
        AtomicInteger handlerCalls = new AtomicInteger();
        timer.setTaskExceptionHandler((task, e) -> handlerCalls.incrementAndGet());

        timer.schedule(() -> {
            throw new IllegalStateException("boom");
        }, (task, e) -> viaCallback.set(e), 1, TimeUnit.MILLISECONDS);

        pumpUntil(() -> viaCallback.get() != null);
        assertThat(viaCallback.get()).hasMessage("boom");
        // the per-task callback took the report, so the handler is not also notified
        assertThat(handlerCalls.get()).isZero();
    }

    @Test
    void aCompletionCallbackThatThrowsIsNotReInvokedWithItsOwnException() throws Exception {
        timer = newTickDrivenTimer();
        // The callback used to sit inside the try that maps a task failure onto the callback, so throwing
        // from the success call fed the callback its own exception as if the task had failed.
        List<String> seen = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<Throwable> reported = new AtomicReference<>();
        timer.setTaskExceptionHandler((task, e) -> reported.set(e));

        timer.schedule(() -> {
        }, (task, e) -> {
            seen.add(e == null ? "success" : "failure:" + e.getMessage());
            throw new IllegalStateException("callback-boom");
        }, 1, TimeUnit.MILLISECONDS);

        pumpUntil(() -> reported.get() != null);
        assertThat(seen).containsExactly("success");
        assertThat(reported.get()).hasMessage("callback-boom");
    }

    // ---------------------------------------------------------------------------------------------------
    // Re-arming a reusable handle from inside its own task — the repeating-timer pattern. The node used to
    // become recyclable before the worker had finished reading it, so the re-arm overwrote the task and
    // callback the worker was about to report on.
    // ---------------------------------------------------------------------------------------------------

    @Test
    void aTaskThatReArmsItsOwnHandleStillGetsItsOwnCompletionCallback() throws Exception {
        timer = newTickDrivenTimer();
        List<String> events = Collections.synchronizedList(new ArrayList<>());
        MutableTimeout handle = timer.newReusableTimeout();

        Runnable second = () -> events.add("second-task");
        Runnable first = () -> {
            events.add("first-task");
            timer.schedule(handle, second, (t, e) -> events.add("second-cb"), 1, TimeUnit.MILLISECONDS);
        };

        timer.schedule(handle, first, (t, e) -> events.add("first-cb"), 1, TimeUnit.MILLISECONDS);

        pumpUntil(() -> events.size() >= 4);
        assertThat(events).containsExactly("first-task", "first-cb", "second-task", "second-cb");
    }

    @Test
    void aTaskCanReArmItsOwnHandleRepeatedly() throws Exception {
        timer = newTickDrivenTimer();
        MutableTimeout handle = timer.newReusableTimeout();
        AtomicInteger runs = new AtomicInteger();

        Runnable[] repeating = new Runnable[1];
        repeating[0] = () -> {
            if (runs.incrementAndGet() < 5) {
                timer.schedule(handle, repeating[0], 1, TimeUnit.MILLISECONDS);
            }
        };
        timer.schedule(handle, repeating[0], 1, TimeUnit.MILLISECONDS);

        pumpUntil(() -> runs.get() == 5);
        assertThat(runs.get()).isEqualTo(5);
    }

    @Test
    void isExpiredOnlyBecomesTrueOnceTheCallbackHasFinished() throws Exception {
        timer = newTickDrivenTimer();
        AtomicReference<Boolean> expiredDuringCallback = new AtomicReference<>();
        AtomicReference<Timeout> handle = new AtomicReference<>();

        Timeout timeout = timer.schedule(() -> {
        }, (t, e) -> expiredDuringCallback.set(handle.get().isExpired()), 1, TimeUnit.MILLISECONDS);
        handle.set(timeout);

        pumpUntil(() -> expiredDuringCallback.get() != null);
        // inside the callback the node is still FIRING, so it does not yet look recyclable
        assertThat(expiredDuringCallback.get()).isFalse();
        assertThat(timeout.isExpired()).isTrue();
    }

    @Test
    void aTimeoutBeingRunCannotBeCancelled() throws Exception {
        timer = newTickDrivenTimer();
        AtomicReference<Boolean> cancelDuringTask = new AtomicReference<>();
        AtomicReference<Timeout> handle = new AtomicReference<>();

        Timeout timeout = timer.schedule(() -> cancelDuringTask.set(handle.get().cancel()),
                1, TimeUnit.MILLISECONDS);
        handle.set(timeout);

        pumpUntil(() -> cancelDuringTask.get() != null);
        assertThat(cancelDuringTask.get()).isFalse();
        assertThat(timeout.isCancelled()).isFalse();
        assertThat(timeout.isExpired()).isTrue();
    }

    @Test
    void setTaskExceptionHandlerRejectsNull() {
        timer = newTickDrivenTimer();
        assertThatThrownBy(() -> timer.setTaskExceptionHandler(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void aHandlerThatThrowsDoesNotKillTheWheel() throws Exception {
        timer = newTickDrivenTimer();
        AtomicInteger handlerCalls = new AtomicInteger();
        timer.setTaskExceptionHandler((task, e) -> {
            handlerCalls.incrementAndGet();
            throw new IllegalStateException("handler-boom");
        });

        timer.schedule(() -> {
            throw new IllegalStateException("boom");
        }, 1, TimeUnit.MILLISECONDS);
        pumpUntil(() -> handlerCalls.get() == 1);

        AtomicInteger later = new AtomicInteger();
        timer.schedule(later::incrementAndGet, 1, TimeUnit.MILLISECONDS);
        pumpUntil(() -> later.get() == 1);
    }

    @Test
    void anErrorFromTheTaskDoesNotReachTheCompletionCallbackAsASuccess() throws Exception {
        timer = newTickDrivenTimer();
        // The callback reports a task outcome as (task, null) or (task, exception); an Error is neither, so
        // it must go to the handler alone rather than being dressed up as a successful completion.
        List<String> seen = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<Throwable> reported = new AtomicReference<>();
        timer.setTaskExceptionHandler((task, e) -> reported.set(e));

        timer.schedule(() -> {
            throw new StackOverflowError("deep");
        }, (task, e) -> seen.add(e == null ? "success" : "failure"), 1, TimeUnit.MILLISECONDS);

        pumpUntil(() -> reported.get() != null);
        assertThat(reported.get()).isInstanceOf(StackOverflowError.class);
        assertThat(seen).isEmpty();
    }

    // ---------------------------------------------------------------------------------------------------
    // Shutdown. The wheel arrays and the submission ring buffer are single-consumer, so stop() must never
    // touch them from another thread while a worker is alive; and a schedule racing a stop must still end
    // in a terminal state rather than being stranded in the submission buffer.
    // ---------------------------------------------------------------------------------------------------

    private void awaitTrue(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline && !condition.getAsBoolean()) {
            Thread.sleep(1);
        }
        assertThat(condition.getAsBoolean()).as("condition never reached").isTrue();
    }

    @Test
    void stopLeavesTheWheelToAnOverrunningWorkerAndTheWorkerCleansUpOnItsWayOut() throws Exception {
        timer = new WheelTimer(TimeUnit.MILLISECONDS.toNanos(1), 16, 64, false);
        timer.startOwnThread(backoff(), threadFactory());

        // A task that ignores interrupts, so the worker cannot escape within stop()'s timeout.
        CountDownLatch inTask = new CountDownLatch(1);
        AtomicBoolean release = new AtomicBoolean();
        timer.schedule(() -> {
            inTask.countDown();
            while (!release.get()) {
                Thread.onSpinWait();
            }
        }, 1, TimeUnit.MILLISECONDS);
        assertThat(inTask.await(2, TimeUnit.SECONDS)).isTrue();

        Timeout pending = timer.schedule(() -> {
        }, 500, TimeUnit.MILLISECONDS);

        long startedAt = System.nanoTime();
        timer.stop(Duration.ofMillis(100));
        long tookMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);

        // stop() gives up on the join rather than blocking for ever...
        assertThat(tookMillis).isLessThan(2_000L);
        // ...and leaves the wheel alone, because the wedged worker still owns it. Draining from here is
        // exactly the concurrent mutation this guards against.
        assertThat(pending.isCancelled()).isFalse();

        // Once the worker escapes it cancels the outstanding work itself, on the tick thread.
        release.set(true);
        awaitTrue(pending::isCancelled);
        timer = null;
    }

    @Test
    void aScheduleRacingStopAlwaysEndsInATerminalState() throws Exception {
        // A stop() draining the submission buffer just after an offer landed used to strand that node
        // there for ever: never run, and reporting neither expired, nor cancelled, nor rejected, so a
        // caller waiting on the handle waited for ever.
        for (int attempt = 0; attempt < 30; attempt++) {
            timer = new WheelTimer(TimeUnit.MILLISECONDS.toNanos(1), 16, 1024, true);
            timer.startOwnThread(backoff(), threadFactory());

            List<Timeout> handles = Collections.synchronizedList(new ArrayList<>());
            int producers = 4;
            CountDownLatch go = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(producers);
            ExecutorService pool = Executors.newFixedThreadPool(producers);
            try {
                for (int i = 0; i < producers; i++) {
                    pool.submit(() -> {
                        try {
                            go.await();
                            for (int n = 0; n < 200; n++) {
                                handles.add(timer.schedule(() -> {
                                }, 5, TimeUnit.SECONDS));
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            done.countDown();
                        }
                    });
                }
                go.countDown();
                timer.stop(Duration.ofSeconds(2));
                assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
            } finally {
                pool.shutdownNow();
            }

            for (Timeout handle : handles) {
                assertThat(handle.isRejected() || handle.isCancelled() || handle.isExpired())
                        .as("schedule/stop race left a timeout in a non-terminal state")
                        .isTrue();
            }
            timer = null;
        }
    }

    @Test
    void concurrentStopsDoNotBothDrainTheWheel() throws Exception {
        timer = new WheelTimer(TimeUnit.MILLISECONDS.toNanos(1), 16, 1024, true);
        timer.start();

        List<Timeout> handles = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            handles.add(timer.schedule(() -> {
            }, 5, TimeUnit.SECONDS));
        }
        timer.tick(); // move them out of the submission buffer and into the wheel

        int stoppers = 4;
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(stoppers);
        List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
        ExecutorService pool = Executors.newFixedThreadPool(stoppers);
        try {
            for (int i = 0; i < stoppers; i++) {
                pool.submit(() -> {
                    try {
                        go.await();
                        timer.stop(Duration.ofSeconds(1));
                    } catch (Throwable t) {
                        failures.add(t);
                    } finally {
                        done.countDown();
                    }
                });
            }
            go.countDown();
            assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }

        assertThat(failures).isEmpty();
        assertThat(handles).allMatch(Timeout::isCancelled);
        timer = null;
    }

    @Test
    void aWorkerThatDiesMarksTheTimerStoppedSoLaterSchedulesAreRejected() throws Exception {
        timer = new WheelTimer(TimeUnit.MILLISECONDS.toNanos(1), 16, 64, false);
        // An idle strategy that blows up is the one way to make the worker loop itself fail.
        CountDownLatch died = new CountDownLatch(1);
        ThreadFactory factory = r -> {
            Thread t = new Thread(r, "wheel-timer-dying");
            t.setDaemon(true);
            t.setUncaughtExceptionHandler((thread, e) -> died.countDown());
            return t;
        };
        timer.startOwnThread(new IdleStrategy() {
            @Override
            public void idle(int workCount) {
                throw new IllegalStateException("idle-boom");
            }

            @Override
            public void idle() {
                throw new IllegalStateException("idle-boom");
            }

            @Override
            public void reset() {
            }
        }, factory);

        assertThat(died.await(5, TimeUnit.SECONDS)).isTrue();
        awaitTrue(() -> !timer.isStarted());
        assertThat(timer.schedule(() -> {
        }, 1, TimeUnit.MILLISECONDS).isRejected()).isTrue();
        timer = null;
    }

    @Test
    void aTickDrivenRunAfterAnOverrunningWorkerStillCancelsItsTasksOnStop() throws Exception {
        timer = new WheelTimer(TimeUnit.MILLISECONDS.toNanos(1), 16, 64, false);
        timer.startOwnThread(backoff(), threadFactory());

        // Wedge the worker so the first stop() overruns and leaves it holding the wheel.
        CountDownLatch inTask = new CountDownLatch(1);
        AtomicBoolean release = new AtomicBoolean();
        timer.schedule(() -> {
            inTask.countDown();
            while (!release.get()) {
                Thread.onSpinWait();
            }
        }, 1, TimeUnit.MILLISECONDS);
        assertThat(inTask.await(2, TimeUnit.SECONDS)).isTrue();
        timer.stop(Duration.ofMillis(100));
        release.set(true);
        awaitTrue(() -> !timer.isStarted());

        // Restart tick-driven. The dead worker must not make this run's stop() skip its cleanup.
        timer.start();
        Timeout pending = timer.schedule(() -> {
        }, 5, TimeUnit.SECONDS);
        timer.tick();

        timer.stop(Duration.ofSeconds(1));
        assertThat(pending.isCancelled()).isTrue();
        timer = null;
    }
}
