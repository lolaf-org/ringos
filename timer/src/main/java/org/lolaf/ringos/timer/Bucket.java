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

import java.util.Arrays;
import java.util.function.BiConsumer;

/**
 * One slot of the hashed wheel. Stores its {@link TimeoutNode}s in a contiguous array so the
 * per-tick {@link #expire(TimerTaskExceptionHandler)} walk is sequential in memory (good prefetch / cache behavior).
 * Touched exclusively by the timer worker thread (after the submission ring buffer hand-off),
 * so all operations are plain field reads/writes — no synchronization needed.
 * <p>
 * The array is lazy-allocated on first {@link #add} so empty buckets cost a single null
 * reference. On overflow the array doubles (amortized O(1) growth). Removal during
 * {@link #expire(TimerTaskExceptionHandler)} uses swap-with-last to keep deletion O(1); bucket order is not preserved
 * but no caller depends on it.
 */
final class Bucket {

    private static final int INITIAL_CAPACITY = 16;

    private TimeoutNode[] entries;
    private int size;

    void add(TimeoutNode node) {
        if (entries == null) {
            entries = new TimeoutNode[INITIAL_CAPACITY];
        } else if (size == entries.length) {
            entries = Arrays.copyOf(entries, entries.length * 2);
        }
        entries[size++] = node;
    }

    /**
     * Walks the bucket once and fires any node whose {@code remainingRounds} reaches zero.
     * Cancelled nodes are removed without firing. Surviving nodes have their
     * {@code remainingRounds} decremented and stay in place.
     *
     * @param exceptionHandler notified when a task, or a task's completion callback, throws
     * @return number of tasks that fired (excludes cancelled nodes and nodes still pending
     * further rotations)
     */
    int expire(TimerTaskExceptionHandler exceptionHandler) {
        int fired = 0;
        int i = 0;
        while (i < size) {
            TimeoutNode node = entries[i];
            if (node.isCancelled()) {
                swapRemove(i);
            } else if (node.remainingRounds <= 0) {
                swapRemove(i);
                if (node.tryMarkFiring()) {
                    fired++;
                    run(node, exceptionHandler);
                }
            } else {
                node.remainingRounds--;
                i++;
            }
        }
        return fired;
    }

    /**
     * Drains all remaining nodes, marking each as cancelled (used at shutdown).
     */
    void drain() {
        for (int i = 0; i < size; i++) {
            entries[i].cancel();
            entries[i] = null;
        }
        size = 0;
    }

    /**
     * Runs one claimed node's task and then reports its outcome, exactly once.
     * <p>
     * Everything the node holds is snapshotted before the task runs, because the task may re-arm this very
     * node — the repeating-timer pattern, legal from {@code STATE_FIRING}. That overwrites the node's task,
     * callback and arguments for the <em>next</em> cycle, and the outcome we are about to report belongs to
     * the task that just ran.
     */
    @SuppressWarnings("unchecked")
    private static void run(TimeoutNode node, TimerTaskExceptionHandler exceptionHandler) {
        Object task = node.task;
        BiConsumer<Runnable, Exception> callback = node.taskCallback;
        int arity = node.taskArity;
        Object arg1 = node.arg1;
        Object arg2 = node.arg2;
        Object arg3 = node.arg3;

        Exception failure = null;
        Throwable error = null;
        try {
            switch (arity) {
                case TimeoutNode.ARITY_RUNNABLE:
                    ((Runnable) task).run();
                    break;
                case TimeoutNode.ARITY_ONE_ARG:
                    ((TimerTaskOneArg<Object>) task).execute(arg1);
                    break;
                case TimeoutNode.ARITY_TWO_ARG:
                    ((TimerTaskTwoArg<Object, Object>) task).execute(arg1, arg2);
                    break;
                case TimeoutNode.ARITY_THREE_ARG:
                    ((TimerTaskThreeArg<Object, Object, Object>) task).execute(arg1, arg2, arg3);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            failure = e;
        } catch (Throwable t) {
            error = t;
        }

        // Reporting sits outside the try above, so that a completion callback which throws is never handed
        // its own exception back as if the task had failed, and never invoked a second time.
        if (error != null) {
            // An Error must not kill the worker, but it must not vanish either. The completion callback is
            // skipped: it reports a task outcome as (task, null) or (task, exception), and an Error is
            // neither.
            notify(exceptionHandler, task, error);
        } else if (callback != null && arity == TimeoutNode.ARITY_RUNNABLE) {
            try {
                callback.accept((Runnable) task, failure);
            } catch (Throwable t) {
                notify(exceptionHandler, task, t);
            }
        } else if (failure != null) {
            // The arity overloads carry no per-task callback, so this is the only report of their failures.
            notify(exceptionHandler, task, failure);
        }

        // Last, so that isExpired() implies the task and its callback are both done. A no-op when the task
        // re-armed the node from inside itself.
        node.markExpiredAfterFiring();
    }

    private static void notify(TimerTaskExceptionHandler exceptionHandler, Object task, Throwable thrown) {
        try {
            exceptionHandler.onException(task, thrown);
        } catch (Throwable ignored) {
            // A broken handler must not kill the worker either, and there is nowhere left to report to.
        }
    }

    private void swapRemove(int i) {
        int last = --size;
        entries[i] = entries[last];
        entries[last] = null;
    }
}
