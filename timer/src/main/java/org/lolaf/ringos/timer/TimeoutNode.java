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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * Concrete {@link Timeout} implementation held inside a {@link Bucket}'s entries array. A
 * single instance travels from {@link WheelTimer#schedule} (publisher side) through the
 * submission ring buffer into the wheel bucket (consumer side), so cancellation from any
 * thread observes the same node the worker later inspects.
 * <p>
 * Also doubles as the reusable handle behind {@link MutableTimeout}. The fields are non-final
 * to support {@link #setup(long, Runnable, BiConsumer)}; reset is only valid in terminal
 * states (see {@link MutableTimeout}) and always happens before the node is re-published to
 * the submission ring buffer, so the ring buffer's release semantics carry the new field
 * values to the worker thread.
 */
final class TimeoutNode implements MutableTimeout {

    static final int ARITY_RUNNABLE = 0;
    static final int ARITY_ONE_ARG = 1;
    static final int ARITY_TWO_ARG = 2;
    static final int ARITY_THREE_ARG = 3;
    private static final int STATE_INIT = 0;
    private static final int STATE_CANCELLED = 1;
    private static final int STATE_EXPIRED = 2;
    private static final int STATE_REJECTED = 3;
    /**
     * Fresh-from-factory state for reusable handles; not yet scheduled.
     */
    private static final int STATE_NEW = 4;
    /**
     * The worker has claimed this node and is running its task. Distinct from {@link #STATE_EXPIRED},
     * which is only reached once the task <em>and</em> its completion callback have finished — so a node
     * the worker is still reading never looks recyclable to another thread.
     */
    private static final int STATE_FIRING = 5;
    private static final Runnable NOOP = () -> {
    };
    private final AtomicInteger currentState;
    Object task;
    BiConsumer<Runnable, Exception> taskCallback;
    Object arg1;
    Object arg2;
    Object arg3;
    int taskArity;
    // worker-thread-only field — no synchronization
    long remainingRounds;
    private long deadlineNanos;

    private TimeoutNode(long deadlineNanos, Object task, BiConsumer<Runnable, Exception> taskCallback,
                        Object arg1, Object arg2, Object arg3, int taskArity, int state) {
        this.deadlineNanos = deadlineNanos;
        this.task = task;
        this.taskCallback = taskCallback;
        this.arg1 = arg1;
        this.arg2 = arg2;
        this.arg3 = arg3;
        this.taskArity = taskArity;
        this.currentState = new AtomicInteger(state);
    }

    TimeoutNode(long deadlineNanos, Runnable task, BiConsumer<Runnable, Exception> taskCallback) {
        this(deadlineNanos, task, taskCallback, null, null, null, ARITY_RUNNABLE, STATE_INIT);
    }

    <A> TimeoutNode(long deadlineNanos, TimerTaskOneArg<A> task, A arg1) {
        this(deadlineNanos, task, null, arg1, null, null, ARITY_ONE_ARG, STATE_INIT);
    }

    <A, B> TimeoutNode(long deadlineNanos, TimerTaskTwoArg<A, B> task, A arg1, B arg2) {
        this(deadlineNanos, task, null, arg1, arg2, null, ARITY_TWO_ARG, STATE_INIT);
    }

    <A, B, C> TimeoutNode(long deadlineNanos, TimerTaskThreeArg<A, B, C> task, A arg1, B arg2, C arg3) {
        this(deadlineNanos, task, null, arg1, arg2, arg3, ARITY_THREE_ARG, STATE_INIT);
    }

    public static TimeoutNode reusable() {
        return new TimeoutNode(0L, NOOP, null, null, null, null, ARITY_RUNNABLE, TimeoutNode.STATE_NEW);
    }

    @Override
    public long getDeadlineNanos() {
        return deadlineNanos;
    }

    @Override
    public boolean cancel() {
        return currentState.compareAndSet(STATE_INIT, STATE_CANCELLED);
    }

    @Override
    public boolean isCancelled() {
        return currentState.get() == STATE_CANCELLED;
    }

    @Override
    public boolean isExpired() {
        return currentState.get() == STATE_EXPIRED;
    }

    @Override
    public boolean isRejected() {
        return currentState.get() == STATE_REJECTED;
    }

    /**
     * Called only from the timer worker thread when this node has been chosen to fire. The CAS guards
     * against a racing {@link #cancel()} — if the user cancels just before we claim the node, we honor the
     * cancel and skip the task.
     *
     * @return {@code true} if this node is now ours to run
     */
    boolean tryMarkFiring() {
        return currentState.compareAndSet(STATE_INIT, STATE_FIRING);
    }

    /**
     * Called only from the timer worker thread, once the task and its completion callback have both
     * finished. The CAS is what makes re-arming a handle from inside its own task safe: that re-arm has
     * already moved the node back to {@link #STATE_INIT} for its next cycle, so this must not stamp
     * {@link #STATE_EXPIRED} over it.
     *
     * @return {@code true} if the node reached {@link #STATE_EXPIRED}, {@code false} if the task re-armed it
     */
    boolean markExpiredAfterFiring() {
        return currentState.compareAndSet(STATE_FIRING, STATE_EXPIRED);
    }

    /**
     * CAS rather than a plain set: a {@code stop()} draining the submission buffer may already have
     * cancelled this node, and both states mean the same thing to the caller — it will never fire. The
     * first one to land wins rather than the last.
     */
    void markRejected() {
        currentState.compareAndSet(STATE_INIT, STATE_REJECTED);
    }

    /**
     * Prepares this node for a fresh scheduling cycle. Only legal from a state where the worker is not
     * about to read the node: NEW (fresh from factory), EXPIRED (already fired), REJECTED (prior schedule
     * never entered the wheel), or FIRING (the task is re-arming itself, from the worker thread — the node
     * has already left its bucket and the worker snapshotted everything it still needs). Throws on
     * CANCELLED or INIT to surface unsafe recycling loudly.
     */
    @Override
    public TimeoutNode setup(long deadlineNanos, Runnable task, BiConsumer<Runnable, Exception> taskCallback) {
        checkRecyclable();
        this.deadlineNanos = deadlineNanos;
        this.task = task;
        this.taskCallback = taskCallback;
        this.arg1 = null;
        this.arg2 = null;
        this.arg3 = null;
        this.taskArity = ARITY_RUNNABLE;
        this.remainingRounds = 0L;
        this.currentState.set(STATE_INIT);
        return this;
    }

    @Override
    public <A> TimeoutNode setup(long deadlineNanos, TimerTaskOneArg<A> task, A arg1) {
        checkRecyclable();
        this.deadlineNanos = deadlineNanos;
        this.task = task;
        this.taskCallback = null;
        this.arg1 = arg1;
        this.arg2 = null;
        this.arg3 = null;
        this.taskArity = ARITY_ONE_ARG;
        this.remainingRounds = 0L;
        this.currentState.set(STATE_INIT);
        return this;
    }

    @Override
    public <A, B> TimeoutNode setup(long deadlineNanos, TimerTaskTwoArg<A, B> task, A arg1, B arg2) {
        checkRecyclable();
        this.deadlineNanos = deadlineNanos;
        this.task = task;
        this.taskCallback = null;
        this.arg1 = arg1;
        this.arg2 = arg2;
        this.arg3 = null;
        this.taskArity = ARITY_TWO_ARG;
        this.remainingRounds = 0L;
        this.currentState.set(STATE_INIT);
        return this;
    }

    @Override
    public <A, B, C> TimeoutNode setup(long deadlineNanos, TimerTaskThreeArg<A, B, C> task, A arg1, B arg2, C arg3) {
        checkRecyclable();
        this.deadlineNanos = deadlineNanos;
        this.task = task;
        this.taskCallback = null;
        this.arg1 = arg1;
        this.arg2 = arg2;
        this.arg3 = arg3;
        this.taskArity = ARITY_THREE_ARG;
        this.remainingRounds = 0L;
        this.currentState.set(STATE_INIT);
        return this;
    }

    private void checkRecyclable() {
        int s = currentState.get();
        if (s != STATE_NEW && s != STATE_EXPIRED && s != STATE_REJECTED && s != STATE_FIRING) {
            throw new IllegalStateException("Cannot recycle MutableTimeout: still in use (state=" + s + ")");
        }
    }
}
