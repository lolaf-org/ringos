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

import lombok.AccessLevel;
import lombok.Getter;

import java.util.concurrent.locks.LockSupport;

/**
 * Escalates through three phases the longer a thread finds nothing to do: it spins, then yields, then parks for a
 * period that doubles up to a ceiling. The general-purpose choice — it reacts in nanoseconds to work that arrives
 * straight away, and costs almost nothing once a queue has been quiet for a while.
 *
 * <p>Any progress rewinds the escalation: {@link #idle(int)} with a non-zero work count calls {@link #reset()}, so
 * a burst of work leaves the strategy spinning again rather than parked at its longest period.
 *
 * <p>The default minimum park period is 50 microseconds because a shorter one buys nothing: the kernel's default
 * timer slack is around that figure, so it would round the park back up anyway. Going below it takes
 * {@link TimerSlackAwareBackoffIdleStrategy}, which narrows the slack on the idling thread first.
 *
 * <p>Each instance carries the escalation state of a single idling thread and must not be shared between threads.
 */
public class BackoffIdleStrategy implements IdleStrategy {

    /**
     * Spins done before the strategy starts yielding, when no count is given.
     */
    public static final long DEFAULT_MAX_SPINS = 10L;
    /**
     * Yields done before the strategy starts parking, when no count is given.
     */
    public static final long DEFAULT_MAX_YIELDS = 5L;
    /**
     * First park period when none is given, in nanoseconds. Set at the kernel's usual default timer slack, since
     * a shorter park would be rounded up to it anyway — see {@link TimerSlackAwareBackoffIdleStrategy} to go
     * below.
     */
    // 50 micros, useless to go below as default kernel settings for timer slack is usually 50 micros
    public static final long DEFAULT_MIN_PARK_PERIOD_NS = 50 * 1000L;
    /**
     * Ceiling the park period doubles up to when none is given, in nanoseconds.
     */
    public static final long DEFAULT_MAX_PARK_PERIOD_NS = 1_000_000L; // 1 millis
    private static final int SPINNING = 0;
    private static final int YIELDING = 1;
    private static final int PARKING = 2;

    private final long maxSpins;
    private final long maxYields;
    @Getter(AccessLevel.PACKAGE)
    private final long minParkPeriodNs;
    private final long maxParkPeriodNs;

    private int state = SPINNING;
    private long spins;
    private long yields;
    private long parkPeriodNs;

    /**
     * Builds a strategy on the {@code DEFAULT_} constants of this class: 10 spins, then 5 yields, then parks
     * doubling from 50 microseconds to 1 millisecond.
     */
    public BackoffIdleStrategy() {
        this(DEFAULT_MAX_SPINS, DEFAULT_MAX_YIELDS, DEFAULT_MIN_PARK_PERIOD_NS, DEFAULT_MAX_PARK_PERIOD_NS);
    }

    /**
     * Builds a strategy with an explicit escalation profile.
     * <p>
     * Shortening the spin and yield counts reaches the parked, near-free phase sooner at the price of wake-up
     * latency; raising {@code maxParkPeriodNs} lowers the cost of a long quiet spell but lengthens the worst-case
     * delay before the thread notices work.
     *
     * @param maxSpins        how many {@link Thread#onSpinWait()} idles to do before yielding
     * @param maxYields       how many {@link Thread#yield()} idles to do before parking
     * @param minParkPeriodNs the first park period, in nanoseconds; below ~50 microseconds the kernel's timer
     *                        slack absorbs the difference unless the thread's slack was narrowed
     * @param maxParkPeriodNs the ceiling the park period doubles up to, in nanoseconds
     */
    public BackoffIdleStrategy(
            final long maxSpins, final long maxYields, final long minParkPeriodNs, final long maxParkPeriodNs) {
        this.maxSpins = maxSpins;
        this.maxYields = maxYields;
        this.minParkPeriodNs = minParkPeriodNs;
        this.maxParkPeriodNs = maxParkPeriodNs;
    }

    @Override
    public void idle(final int workCount) {
        if (workCount > 0) {
            reset();
        } else {
            idle();
        }
    }

    @Override
    public void idle() {
        switch (state) {
            case SPINNING:
                if (++spins > maxSpins) {
                    state = YIELDING;
                    yields = 0;
                } else {
                    Thread.onSpinWait();
                }
                break;

            case YIELDING:
                if (++yields > maxYields) {
                    state = PARKING;
                    parkPeriodNs = minParkPeriodNs;
                } else {
                    Thread.yield();
                }
                break;

            case PARKING:
                LockSupport.parkNanos(parkPeriodNs);
                parkPeriodNs = Math.min(parkPeriodNs << 1, maxParkPeriodNs);
                break;
        }
    }

    @Override
    public void reset() {
        spins = 0;
        yields = 0;
        parkPeriodNs = minParkPeriodNs;
        state = SPINNING;
    }
}