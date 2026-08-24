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
package org.lolaf.ringos;

import lombok.ToString;
import org.lolaf.ringos.idling.BackoffIdleStrategy;
import org.lolaf.ringos.idling.IdleStrategy;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * A time budget a caller can hand down through a chain of calls, so that each step waits on what is left of the
 * original allowance rather than on a fresh timeout of its own.
 *
 * <p>Three kinds, all built through the static factories: a {@link #of(Duration) timed} budget that expires,
 * an {@link #unlimited() unlimited} one that never does, and an {@link #immediate() immediate} one that already
 * has. The last two are what let a caller express "block as long as it takes" and "don't block at all" without
 * the callee special-casing either — every accessor answers sensibly for all three.
 *
 * <p>{@link #fromRemainingTime(double)} carves a fraction of the remaining budget off for a sub-step, and
 * {@link #pause()}/{@link #resume()} stop the clock across a stretch that should not be charged to it. Elapsed
 * time is measured from construction, so a deadline is created where the budget starts, not where it is used.
 *
 * <p>Not thread-safe: pausing and resuming mutate the instance, so a deadline shared across threads must be
 * confined to one of them or only ever read.
 */
@ToString
public class Deadline {
    private final Instant startTime;
    private final Duration timeLimit;
    private final DeadlineType type;
    private Instant pausedAt;
    private Duration pausedDuration;

    private enum DeadlineType {
        TIMED,
        UNLIMITED,
        IMMEDIATE
    }

    /**
     * Private constructor - use static factory methods instead
     */
    private Deadline(Duration timeLimit, DeadlineType type) {
        this.startTime = Instant.now();
        this.timeLimit = timeLimit;
        this.type = type;
        this.pausedAt = null;
        this.pausedDuration = Duration.ZERO;
    }

    /**
     * Creates a deadline with a specific time limit, its clock starting now
     *
     * @param duration the budget, which must be strictly positive
     * @return the new deadline
     * @throws IllegalArgumentException if duration is null, zero or negative
     */
    public static Deadline of(Duration duration) {
        if (duration == null || duration.isNegative() || duration.isZero()) {
            throw new IllegalArgumentException("Duration must be positive");
        }
        return new Deadline(duration, DeadlineType.TIMED);
    }

    /**
     * Creates an unlimited deadline that never expires
     *
     * @return the new deadline
     */
    public static Deadline unlimited() {
        return new Deadline(null, DeadlineType.UNLIMITED);
    }

    /**
     * Creates an immediate deadline that is always expired
     *
     * @return the new deadline
     */
    public static Deadline immediate() {
        return new Deadline(null, DeadlineType.IMMEDIATE);
    }

    /**
     * Creates a new deadline from a percentage of the remaining time of this deadline
     * For timed deadlines: creates a new deadline with the specified percentage of remaining time
     * For unlimited deadlines: creates a new unlimited deadline (percentage is ignored)
     * For immediate deadlines: creates a new immediate deadline (percentage is ignored)
     *
     * @param percentage The percentage of remaining time to use (0.0 to 1.0, e.g., 0.5 for 50%)
     * @return the new deadline, immediate if this one is a timed deadline with no time left
     * @throws IllegalArgumentException if percentage is not between 0.0 and 1.0
     */
    public Deadline fromRemainingTime(double percentage) {
        if (percentage <= 0.0 || percentage > 1.0) {
            throw new IllegalArgumentException("Percentage must be between 0.0 (exclusive) and 1.0 (inclusive)");
        }

        switch (type) {
            case IMMEDIATE:
                return Deadline.immediate();
            case UNLIMITED:
                return Deadline.unlimited();
            case TIMED:
                Duration remaining = getRemainingTime();
                if (remaining.isZero()) {
                    return Deadline.immediate();
                }
                long newMillis = (long) (remaining.toMillis() * percentage);
                return Deadline.of(Duration.ofMillis(newMillis));
            default:
                throw new IllegalStateException("Unknown deadline type");
        }
    }

    /**
     * Wait as long as the given condition is true or the remaining deadline is exhausted
     *
     * @param waitCondition the wait condition
     * @return true if the boolean condition not matching anymore or false if we exited the method due to a deadline expiration on the wait condition
     */
    public boolean waitAsLongAs(BooleanSupplier waitCondition) {
        IdleStrategy idleStrategy = new BackoffIdleStrategy(0, 0, TimeUnit.MICROSECONDS.toNanos(100), TimeUnit.MILLISECONDS.toNanos(2));
        if (type == DeadlineType.TIMED) {
            long deadlineInNanos = System.nanoTime() + getRemainingTime().toNanos();
            while (System.nanoTime() < deadlineInNanos && waitCondition.getAsBoolean()) {
                idleStrategy.idle();
            }
        } else if (type == DeadlineType.UNLIMITED) {
            while (waitCondition.getAsBoolean()) {
                idleStrategy.idle();
            }
        }
        return !waitCondition.getAsBoolean();
    }

    /**
     * Returns the total elapsed time (excluding paused time)
     * Returns Duration.ZERO for immediate deadlines
     *
     * @return the elapsed time, frozen at the pause point while this deadline is paused
     */
    public Duration getElapsedTime() {
        if (type == DeadlineType.IMMEDIATE) {
            return Duration.ZERO;
        }

        if (pausedAt != null) {
            return Duration.between(startTime, pausedAt).minus(pausedDuration);
        }
        return Duration.between(startTime, Instant.now()).minus(pausedDuration);
    }

    /**
     * Returns the remaining time until deadline
     * Returns Duration.ZERO for immediate or expired deadlines
     * Returns a very large duration for unlimited deadlines
     *
     * @return the remaining time, never negative
     */
    public Duration getRemainingTime() {
        if (type == DeadlineType.IMMEDIATE) {
            return Duration.ZERO;
        }

        if (type == DeadlineType.UNLIMITED) {
            return Duration.ofDays(365000); // Effectively infinite
        }

        Duration remaining = timeLimit.minus(getElapsedTime());
        return remaining.isNegative() ? Duration.ZERO : remaining;
    }

    /**
     * Checks if the deadline has been exceeded
     * Always returns false for unlimited deadlines
     * Always returns true for immediate deadlines
     *
     * @return true if the budget is spent
     */
    public boolean isExpired() {
        switch (type) {
            case IMMEDIATE:
                return true;
            case TIMED:
                return getElapsedTime().compareTo(timeLimit) >= 0;
            default:
                return false;
        }
    }

    /**
     * Pauses the timer, so the time until {@link #resume()} is not charged to the budget
     * (no effect on immediate deadlines, or if already paused)
     */
    public void pause() {
        if (type == DeadlineType.IMMEDIATE) {
            return;
        }

        if (pausedAt == null) {
            pausedAt = Instant.now();
        }
    }

    /**
     * Resumes the timer after being paused (no effect on immediate deadlines, or if not paused)
     */
    public void resume() {
        if (type == DeadlineType.IMMEDIATE) {
            return;
        }

        if (pausedAt != null) {
            pausedDuration = pausedDuration.plus(Duration.between(pausedAt, Instant.now()));
            pausedAt = null;
        }
    }

    /**
     * Returns true if the timer is currently paused
     * Always returns false for immediate deadlines
     *
     * @return true if paused
     */
    public boolean isPaused() {
        if (type == DeadlineType.IMMEDIATE) {
            return false;
        }
        return pausedAt != null;
    }

    /**
     * Returns true if this is an unlimited deadline
     *
     * @return true if this deadline never expires
     */
    public boolean isUnlimited() {
        return type == DeadlineType.UNLIMITED;
    }

    /**
     * Returns true if this is an immediate deadline
     *
     * @return true if this deadline is already expired
     */
    public boolean isImmediate() {
        return type == DeadlineType.IMMEDIATE;
    }

    /**
     * Returns true if this is a timed deadline
     *
     * @return true if this deadline has a finite budget
     */
    public boolean isTimed() {
        return type == DeadlineType.TIMED;
    }
}