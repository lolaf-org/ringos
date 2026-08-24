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

import java.util.function.BiConsumer;

/**
 * Opaque, reusable {@link Timeout} handle returned by the timer's reusable-timeout factory.
 * Lets callers avoid allocating a fresh timeout object per schedule on hot paths.
 * <p>
 * Recycling is only safe once the previous schedule reached a terminal state:
 * {@link #isExpired()} after firing, or {@link #isRejected()} if the submission queue was full
 * (or the timer wasn't started). Scheduling a handle whose previous use is still in flight or
 * was {@link #isCancelled()} throws {@link IllegalStateException} — there is no barrier
 * guaranteeing the worker has finished with a cancelled node, so reusing it would race.
 * <p>
 * The one exception is re-arming a handle from inside its own task, which is how a repeating timer is
 * built. That runs on the worker thread, after the node has left its bucket and after the worker has
 * snapshotted everything it still needs, so it is safe and is allowed: the task that just ran still gets
 * its own completion callback, and the re-armed handle goes on to its next cycle.
 * <p>
 * Only valid instances are those returned by the timer implementation; user code should not
 * implement this interface.
 */
public interface MutableTimeout extends Timeout {

    /**
     * Re-arms this handle for a new scheduling cycle.
     *
     * @param deadlineNanos the deadline, in {@link System#nanoTime()} units
     * @param task          the task to run
     * @param taskCallback  the completion callback, or {@code null} if none is wanted
     * @return this handle
     * @throws IllegalStateException if the previous use has not reached a terminal state
     */
    MutableTimeout setup(long deadlineNanos, Runnable task, BiConsumer<Runnable, Exception> taskCallback);

    /**
     * Re-arms this handle for a one-argument task. Avoids allocating a capturing lambda when the
     * task is held in a static field.
     *
     * @param deadlineNanos the deadline, in {@link System#nanoTime()} units
     * @param task          the task to execute
     * @param arg1          the argument passed to the task on expiry
     * @param <A>           the argument type
     * @return this handle
     * @throws IllegalStateException if the previous use has not reached a terminal state
     */
    <A> MutableTimeout setup(long deadlineNanos, TimerTaskOneArg<A> task, A arg1);

    /**
     * Re-arms this handle for a two-argument task. Avoids allocating a capturing lambda when the
     * task is held in a static field.
     *
     * @param deadlineNanos the deadline, in {@link System#nanoTime()} units
     * @param task          the task to execute
     * @param arg1          the first argument passed to the task on expiry
     * @param arg2          the second argument passed to the task on expiry
     * @param <A>           the first argument type
     * @param <B>           the second argument type
     * @return this handle
     * @throws IllegalStateException if the previous use has not reached a terminal state
     */
    <A, B> MutableTimeout setup(long deadlineNanos, TimerTaskTwoArg<A, B> task, A arg1, B arg2);

    /**
     * Re-arms this handle for a three-argument task. Avoids allocating a capturing lambda when the
     * task is held in a static field.
     *
     * @param deadlineNanos the deadline, in {@link System#nanoTime()} units
     * @param task          the task to execute
     * @param arg1          the first argument passed to the task on expiry
     * @param arg2          the second argument passed to the task on expiry
     * @param arg3          the third argument passed to the task on expiry
     * @param <A>           the first argument type
     * @param <B>           the second argument type
     * @param <C>           the third argument type
     * @return this handle
     * @throws IllegalStateException if the previous use has not reached a terminal state
     */
    <A, B, C> MutableTimeout setup(long deadlineNanos, TimerTaskThreeArg<A, B, C> task, A arg1, B arg2, C arg3);

}
