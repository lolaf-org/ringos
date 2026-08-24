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
package org.lolaf.ringos.idling;

import java.time.Duration;
import java.util.Objects;

/**
 * Bounded variant of {@link WaitNotifyIdleStrategy}. {@link #idle()} blocks on {@code Object.wait}
 * for at most a configurable duration before returning, so callers that rely on time advancing on
 * their own — such as a {@code WheelTimer} worker that needs to ride its own ticks — won't get
 * stuck indefinitely between {@link #wakeup} calls.
 *
 * <p>Use this when:
 * <ul>
 *   <li>You want the parked-thread efficiency of wait/notify (no spinning, no busy polling), and</li>
 *   <li>The owning component must still progress on a periodic schedule, not only on external
 *       wakeup signals.</li>
 * </ul>
 * The plain {@link WaitNotifyIdleStrategy} blocks indefinitely until {@link #wakeup} is called, so
 * pairing it with a wheel timer (or any time-driven loop) silently deadlocks the worker.
 *
 * <p>Pick {@code maxWait} relative to the loop's resolution requirement — for a wheel timer, a
 * value at or below {@code tickDurationNanos} keeps tick latency bounded.
 */
public class TimedWaitNotifyIdleStrategy implements IdleStrategy {

    private final long maxWaitMillis;
    private final int maxWaitNanos;

    /**
     * @param maxWait how long a single {@link #idle()} may block before returning on its own; pick it from the
     *                loop's resolution requirement, at or below a wheel timer's tick duration for instance
     * @throws NullPointerException     if {@code maxWait} is {@code null}
     * @throws IllegalArgumentException if {@code maxWait} is zero or negative, which {@code Object.wait} would
     *                                  read as "wait forever"
     */
    public TimedWaitNotifyIdleStrategy(Duration maxWait) {
        Objects.requireNonNull(maxWait, "maxWait");
        if (maxWait.isZero() || maxWait.isNegative()) {
            throw new IllegalArgumentException("maxWait must be strictly positive, got " + maxWait);
        }
        long totalNanos = maxWait.toNanos();
        this.maxWaitMillis = totalNanos / 1_000_000L;
        this.maxWaitNanos = (int) (totalNanos % 1_000_000L);
    }

    @Override
    public void idle(int workCount) {
        if (workCount == 0) {
            idle();
        }
    }

    @Override
    public void idle() {
        try {
            synchronized (this) {
                // Object.wait(0, 0) means "wait forever" — TimedWaitNotifyIdleStrategy's constructor
                // rejects zero/negative durations so we always hit the bounded branch here.
                this.wait(maxWaitMillis, maxWaitNanos);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void reset() {
        // nothing to do
    }

    @Override
    public void wakeup() {
        synchronized (this) {
            this.notify();
        }
    }
}
