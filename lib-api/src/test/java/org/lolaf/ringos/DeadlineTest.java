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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Timing tests are kept short by relying on two properties rather than on long sleeps:
 * <ul>
 *     <li>{@link Thread#sleep(long)} never returns early, so an elapsed time can always be
 *     asserted to be at least the slept duration, with {@link #JITTER} as the only slack;</li>
 *     <li>a paused deadline derives its elapsed/remaining time from fixed fields, so its
 *     measurements are exactly reproducible and can be compared with {@code isEqualTo}.</li>
 * </ul>
 */
class DeadlineTest {

    /** Sleep unit: long enough to be measurable, short enough to keep the suite fast. */
    private static final Duration TICK = Duration.ofMillis(50);

    /** Upper slack allowed on any wall-clock measurement (GC, scheduling, loaded CI). */
    private static final Duration JITTER = Duration.ofMillis(500);

    /** Slack allowed between two back-to-back calls, where only bookkeeping happens. */
    private static final Duration SLACK = Duration.ofMillis(250);

    /** A limit no test can reach, so the deadline stays live for the whole test. */
    private static final Duration NEVER_REACHED = Duration.ofSeconds(10);

    @Nested
    class Factories {

        @Test
        void ofCreatesATimedDeadline() {
            Deadline deadline = Deadline.of(NEVER_REACHED);

            assertIsExactly(deadline, Kind.TIMED);
            assertThat(deadline.isExpired()).isFalse();
            assertThat(deadline.getRemainingTime()).isBetween(NEVER_REACHED.minus(JITTER), NEVER_REACHED);
        }

        @Test
        void unlimitedCreatesADeadlineThatNeverExpires() {
            Deadline deadline = Deadline.unlimited();

            assertIsExactly(deadline, Kind.UNLIMITED);
            assertThat(deadline.isExpired()).isFalse();
            assertThat(deadline.getRemainingTime()).isGreaterThan(Duration.ofDays(100_000));
        }

        @Test
        void immediateCreatesAnAlreadyExpiredDeadline() {
            Deadline deadline = Deadline.immediate();

            assertIsExactly(deadline, Kind.IMMEDIATE);
            assertThat(deadline.isExpired()).isTrue();
            assertThat(deadline.getRemainingTime()).isZero();
            assertThat(deadline.getElapsedTime()).isZero();
        }

        @Test
        void ofRejectsANullDuration() {
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> Deadline.of(null))
                    .withMessage("Duration must be positive");
        }

        @Test
        void ofRejectsANegativeDuration() {
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> Deadline.of(Duration.ofSeconds(-1)))
                    .withMessage("Duration must be positive");
        }

        @Test
        void ofRejectsAZeroDuration() {
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> Deadline.of(Duration.ZERO))
                    .withMessage("Duration must be positive");
        }
    }

    @Nested
    class TimedDeadlines {

        @Test
        void elapsedAndRemainingTimeTrackTheWallClock() throws InterruptedException {
            Deadline deadline = Deadline.of(NEVER_REACHED);

            Thread.sleep(TICK.toMillis());

            Duration elapsed = deadline.getElapsedTime();
            Duration remaining = deadline.getRemainingTime();

            assertThat(elapsed).as("elapsed after sleeping %s", TICK).isBetween(TICK, TICK.plus(JITTER));
            assertThat(elapsed.plus(remaining))
                    .as("elapsed + remaining should add up to the time limit")
                    .isCloseTo(NEVER_REACHED, JITTER);
        }

        @Test
        void expiresOnceTheTimeLimitIsReached() throws InterruptedException {
            Deadline deadline = Deadline.of(TICK);
            assertThat(deadline.isExpired()).as("expired before its limit").isFalse();

            Thread.sleep(TICK.plusMillis(10).toMillis());

            assertThat(deadline.isExpired()).as("expired after its limit").isTrue();
            assertThat(deadline.getElapsedTime()).isGreaterThanOrEqualTo(TICK);
            assertThat(deadline.getRemainingTime())
                    .as("remaining time is clamped to zero, never negative")
                    .isZero();
        }
    }

    @Nested
    class UnlimitedDeadlines {

        @Test
        void neverExpireButStillTrackElapsedTime() throws InterruptedException {
            Deadline deadline = Deadline.unlimited();

            Thread.sleep(TICK.toMillis());

            assertThat(deadline.isExpired()).isFalse();
            assertThat(deadline.getElapsedTime()).isBetween(TICK, TICK.plus(JITTER));
            assertThat(deadline.getRemainingTime()).isGreaterThan(Duration.ofDays(100_000));
        }

        @Test
        void canBePaused() throws InterruptedException {
            Deadline deadline = Deadline.unlimited();

            deadline.pause();
            Duration frozen = deadline.getElapsedTime();
            Thread.sleep(TICK.toMillis());

            assertThat(deadline.isPaused()).isTrue();
            assertThat(deadline.getElapsedTime()).isEqualTo(frozen);
        }
    }

    @Nested
    class ImmediateDeadlines {

        @Test
        void stayExpiredWithZeroElapsedAndRemainingTime() throws InterruptedException {
            Deadline deadline = Deadline.immediate();

            Thread.sleep(TICK.toMillis());

            assertThat(deadline.isExpired()).isTrue();
            assertThat(deadline.getElapsedTime()).isZero();
            assertThat(deadline.getRemainingTime()).isZero();
        }

        @Test
        void ignorePauseAndResume() {
            Deadline deadline = Deadline.immediate();

            deadline.pause();
            assertThat(deadline.isPaused()).as("after pause()").isFalse();

            deadline.resume();
            assertThat(deadline.isPaused()).as("after resume()").isFalse();
        }
    }

    @Nested
    class PauseAndResume {

        @Test
        void isPausedReflectsTheCurrentState() {
            Deadline deadline = Deadline.of(NEVER_REACHED);
            assertThat(deadline.isPaused()).as("freshly created").isFalse();

            deadline.pause();
            assertThat(deadline.isPaused()).as("after pause()").isTrue();

            deadline.resume();
            assertThat(deadline.isPaused()).as("after resume()").isFalse();
        }

        @Test
        void pauseFreezesElapsedAndRemainingTime() throws InterruptedException {
            Deadline deadline = Deadline.of(NEVER_REACHED);

            deadline.pause();
            Duration frozenElapsed = deadline.getElapsedTime();
            Duration frozenRemaining = deadline.getRemainingTime();

            Thread.sleep(TICK.toMillis());

            assertThat(deadline.getElapsedTime()).as("elapsed while paused").isEqualTo(frozenElapsed);
            assertThat(deadline.getRemainingTime()).as("remaining while paused").isEqualTo(frozenRemaining);
        }

        @Test
        void aSecondPauseCallIsIgnored() throws InterruptedException {
            Deadline deadline = Deadline.of(NEVER_REACHED);

            deadline.pause();
            Duration frozenElapsed = deadline.getElapsedTime();

            Thread.sleep(TICK.toMillis());
            deadline.pause();

            assertThat(deadline.isPaused()).isTrue();
            assertThat(deadline.getElapsedTime())
                    .as("a second pause() must not move the pause point")
                    .isEqualTo(frozenElapsed);
        }

        @Test
        void resumeRestartsTheClock() throws InterruptedException {
            Deadline deadline = Deadline.of(NEVER_REACHED);
            deadline.pause();
            Duration frozenElapsed = deadline.getElapsedTime();

            deadline.resume();
            Thread.sleep(TICK.toMillis());

            assertThat(deadline.getElapsedTime()).isGreaterThanOrEqualTo(frozenElapsed.plus(TICK));
        }

        @Test
        void pausedTimeIsNotCountedAsElapsed() throws InterruptedException {
            Deadline deadline = Deadline.of(NEVER_REACHED);

            Thread.sleep(TICK.toMillis());       // running
            deadline.pause();
            Thread.sleep(TICK.multipliedBy(3).toMillis()); // paused, must not be counted
            deadline.resume();
            Thread.sleep(TICK.toMillis());       // running again

            Duration expected = TICK.multipliedBy(2);
            assertThat(deadline.getElapsedTime())
                    .as("only the two running periods should count")
                    .isBetween(expected, expected.plus(JITTER));
        }

        @Test
        void resumeWithoutAPauseIsIgnored() throws InterruptedException {
            Deadline deadline = Deadline.of(NEVER_REACHED);

            deadline.resume();
            Thread.sleep(TICK.toMillis());

            assertThat(deadline.isPaused()).isFalse();
            assertThat(deadline.getElapsedTime()).isBetween(TICK, TICK.plus(JITTER));
        }
    }

    @Nested
    class FromRemainingTime {

        @ParameterizedTest(name = "{0} of the remaining time")
        @ValueSource(doubles = {1.0, 0.5, 0.25, 0.1})
        void derivesTheGivenShareOfTheRemainingTime(double percentage) {
            Deadline original = Deadline.of(NEVER_REACHED);
            original.pause(); // freeze the source so the expected value is exact
            Duration expected = scale(original.getRemainingTime(), percentage);

            Deadline derived = original.fromRemainingTime(percentage);

            assertIsExactly(derived, Kind.TIMED);
            assertThat(derived.isExpired()).isFalse();
            assertThat(derived.getRemainingTime()).isBetween(expected.minus(SLACK), expected);
        }

        @Test
        void usesTheRemainingTimeAndNotTheOriginalTimeLimit() throws InterruptedException {
            Duration limit = TICK.multipliedBy(20); // 1s
            Deadline original = Deadline.of(limit);

            Thread.sleep(TICK.multipliedBy(4).toMillis()); // burn 200ms of the limit
            Deadline derived = original.fromRemainingTime(1.0);

            assertThat(derived.getRemainingTime())
                    .as("derived from the ~800ms left, not from the 1s limit")
                    .isLessThan(limit.minus(TICK.multipliedBy(4)))
                    .isGreaterThan(limit.minus(TICK.multipliedBy(4)).minus(SLACK));
        }

        @Test
        void isIndependentFromTheOriginalDeadline() throws InterruptedException {
            Deadline original = Deadline.of(NEVER_REACHED);
            Deadline derived = original.fromRemainingTime(1.0);

            original.pause();
            Duration frozen = original.getElapsedTime();
            Thread.sleep(TICK.toMillis());

            assertThat(original.getElapsedTime()).as("original stays paused").isEqualTo(frozen);
            assertThat(derived.isPaused()).as("derived is not paused").isFalse();
            assertThat(derived.getElapsedTime()).as("derived keeps ticking").isGreaterThanOrEqualTo(TICK);
        }

        @Test
        void keepsThePausedTimeOfTheOriginalOutOfTheComputation() throws InterruptedException {
            Deadline original = Deadline.of(NEVER_REACHED);

            Thread.sleep(TICK.toMillis());
            original.pause();
            Thread.sleep(TICK.multipliedBy(3).toMillis()); // paused, must not shrink the remaining time

            Deadline derived = original.fromRemainingTime(0.5);

            Duration expected = scale(NEVER_REACHED.minus(TICK), 0.5);
            assertThat(derived.getRemainingTime()).isBetween(expected.minus(JITTER), expected);
        }

        @Test
        void yieldsAnImmediateDeadlineWhenTheOriginalHasExpired() throws InterruptedException {
            Deadline original = Deadline.of(TICK);

            Thread.sleep(TICK.plusMillis(10).toMillis());
            assertThat(original.isExpired()).isTrue();

            assertIsExactly(original.fromRemainingTime(0.5), Kind.IMMEDIATE);
        }

        @Test
        void yieldsAnUnlimitedDeadlineFromAnUnlimitedOne() {
            Deadline derived = Deadline.unlimited().fromRemainingTime(0.5);

            assertIsExactly(derived, Kind.UNLIMITED);
            assertThat(derived.isExpired()).isFalse();
        }

        @Test
        void yieldsAnImmediateDeadlineFromAnImmediateOne() {
            Deadline derived = Deadline.immediate().fromRemainingTime(0.5);

            assertIsExactly(derived, Kind.IMMEDIATE);
            assertThat(derived.isExpired()).isTrue();
        }

        @ParameterizedTest
        @ValueSource(doubles = {0.0, -0.5, 1.5})
        void rejectsAPercentageOutsideOfZeroExclusiveToOneInclusive(double percentage) {
            Deadline original = Deadline.of(NEVER_REACHED);

            assertThatIllegalArgumentException()
                    .isThrownBy(() -> original.fromRemainingTime(percentage))
                    .withMessage("Percentage must be between 0.0 (exclusive) and 1.0 (inclusive)");
        }
    }

    @Nested
    class WaitAsLongAs {

        @Test
        void returnsTrueWhenTheConditionClearsBeforeTheDeadline() {
            Deadline deadline = Deadline.of(NEVER_REACHED);
            Deadline conditionHolds = Deadline.of(TICK);

            assertThat(deadline.waitAsLongAs(() -> !conditionHolds.isExpired())).isTrue();
            assertThat(deadline.isExpired()).as("waited far less than the deadline").isFalse();
        }

        @Test
        void returnsFalseWhenTheDeadlineExpiresFirst() {
            Deadline deadline = Deadline.of(TICK);

            assertThat(deadline.waitAsLongAs(() -> true)).isFalse();
            assertThat(deadline.isExpired()).isTrue();
        }

        @Test
        void waitsWithoutATimeLimitOnAnUnlimitedDeadline() {
            Deadline conditionHolds = Deadline.of(TICK);

            assertThat(Deadline.unlimited().waitAsLongAs(() -> !conditionHolds.isExpired())).isTrue();
            assertThat(conditionHolds.isExpired()).as("waited until the condition cleared").isTrue();
        }

        @Test
        void doesNotWaitAtAllOnAnImmediateDeadline() {
            Deadline deadline = Deadline.immediate();

            long startNanos = System.nanoTime();
            boolean conditionCleared = deadline.waitAsLongAs(() -> true);

            assertThat(conditionCleared).isFalse();
            assertThat(Duration.ofNanos(System.nanoTime() - startNanos)).isLessThan(JITTER);
        }
    }

    private enum Kind {
        TIMED, UNLIMITED, IMMEDIATE
    }

    /** Asserts the deadline reports the given kind, and only that kind. */
    private static void assertIsExactly(Deadline deadline, Kind kind) {
        assertThat(deadline).isNotNull();
        assertThat(deadline.isTimed()).as("isTimed").isEqualTo(kind == Kind.TIMED);
        assertThat(deadline.isUnlimited()).as("isUnlimited").isEqualTo(kind == Kind.UNLIMITED);
        assertThat(deadline.isImmediate()).as("isImmediate").isEqualTo(kind == Kind.IMMEDIATE);
    }

    private static Duration scale(Duration duration, double percentage) {
        return Duration.ofMillis((long) (duration.toMillis() * percentage));
    }
}
