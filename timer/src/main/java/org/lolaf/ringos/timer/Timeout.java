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

/**
 * Handle returned by a timer when a task is scheduled.
 * <p>
 * Cancellation is cooperative: {@link #cancel()} only flags the timeout. The actual unlinking
 * happens on the timer thread when the bucket is next visited, so a cancelled timeout never
 * runs but may briefly remain referenced by the timer's internal state.
 */
public interface Timeout {

    /**
     * @return the deadline, in {@link System#nanoTime()} units, at which the task is meant to fire
     */
    long getDeadlineNanos();

    /**
     * Attempts to cancel the scheduled task.
     *
     * @return {@code true} if this call transitioned the timeout from scheduled to cancelled;
     * {@code false} if it had already fired, been cancelled, or was rejected at submission time.
     */
    boolean cancel();

    /**
     * @return {@code true} once {@link #cancel()} has succeeded on this timeout. The task will not
     * run, even if the timer has not yet unlinked the timeout from its bucket.
     */
    boolean isCancelled();

    /**
     * @return {@code true} once the task and its completion callback have both finished. A timeout the
     * timer has claimed but not yet finished running reports {@code false} here and {@code false} from
     * {@link #isCancelled()} — it is past the point of being cancelled, but not yet safe to recycle.
     */
    boolean isExpired();

    /**
     * @return {@code true} when the timer refused this task at {@code schedule()} time — either its
     * submission queue was full, or the timer was not started. The task is guaranteed to never
     * fire, so the caller is free to retry or to surface the rejection.
     */
    boolean isRejected();
}
