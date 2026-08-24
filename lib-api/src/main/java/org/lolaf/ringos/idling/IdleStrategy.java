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

/**
 * How a thread waits when it has nothing to do — the knob that trades CPU burn against wake-up latency in every
 * ringos polling loop.
 *
 * <p>The implementations shipped here span that trade-off: {@link BusySpinIdleStrategy} never releases the core
 * and wakes in nanoseconds, {@link YieldingIdleStrategy} gives the core up but stays runnable,
 * {@link BackoffIdleStrategy} spins then yields then parks for a growing period, and
 * {@link WaitNotifyIdleStrategy} parks the thread outright until a producer signals it.
 *
 * <p>An instance carries the backoff state of the one thread idling on it, so it is <b>not</b> shareable: give each
 * idling thread its own, except for the stateless singletons ({@link BusySpinIdleStrategy#getInstance()},
 * {@link YieldingIdleStrategy#getInstance()}). {@link #wakeup()} is the exception to that ownership — it is meant
 * to be called by other threads.
 *
 * <p>The expected shape of a caller is poll, then idle on the result, so the strategy sees progress and idleness
 * alike:
 * <pre>{@code
 * idleStrategy.assignToThread(Thread.currentThread());
 * while (running) {
 *     idleStrategy.idle(ringBuffer.poll(consumers));
 * }
 * }</pre>
 */
public interface IdleStrategy {

    /**
     * Idles if the last poll found nothing, and registers progress otherwise.
     * <p>
     * Passing the work count is what lets a backing-off strategy reset its escalation as soon as work shows up,
     * so a busy period does not inherit the parking period a quiet one had reached.
     *
     * @param workCount how many items the caller just processed; {@code 0} means the caller found nothing to do
     */
    void idle(final int workCount);

    /**
     * Idles unconditionally, escalating the wait as a strategy with state sees fit.
     * <p>
     * For the caller that already knows it found no work; {@link #idle(int)} is the form to prefer when the work
     * count is at hand.
     */
    void idle();

    /**
     * Returns the strategy to its least aggressive wait, undoing whatever escalation past idles built up.
     * <p>
     * Called by the polling loop when it starts idling after a spell of work — {@code RingBuffer}'s blocking
     * poll and offer methods do this before their first idle.
     */
    void reset();

    /**
     * Wakes the thread idling on this strategy, if the strategy is one that parks rather than spins.
     * <p>
     * Called by a producer that has just published, from a thread other than the idling one. Strategies that
     * only spin or yield ignore it, which is why the default does nothing: a caller may signal unconditionally
     * without knowing which strategy is in use.
     */
    default void wakeup() {
        // nothing to do by default
    }

    /**
     * Applies whatever thread-level setup this strategy needs, called by the idling thread on itself before its
     * loop starts.
     * <p>
     * Most strategies need none, hence the empty default;
     * {@link TimerSlackAwareBackoffIdleStrategy} uses it to narrow the OS timer slack so its short parks are
     * actually honoured.
     *
     * @param thread the thread that will idle on this strategy
     */
    default void assignToThread(Thread thread) {
        // nothing to do by default
    }
}
