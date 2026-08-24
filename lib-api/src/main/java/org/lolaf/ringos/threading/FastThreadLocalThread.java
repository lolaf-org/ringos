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
package org.lolaf.ringos.threading;

/**
 * A special {@link Thread} that provides fast access to {@link FastThreadLocal} variables.
 * A {@link Thread} carrying its {@link FastThreadLocal} values in a field of its own, which is what lets a lookup
 * be an array index rather than a hash-map probe on {@link ThreadLocal}'s side table.
 *
 * <p>{@link FastThreadLocal} works on any thread, but only reaches its full speed on this one; a plain thread
 * falls back to a regular {@link ThreadLocal} holding the same map. Prefer it for the long-lived polling threads
 * that read thread-locals on the message path.
 *
 * <p>Every constructor wraps the given body so that the thread's {@link FastThreadLocal} values are removed when
 * it finishes, which keeps a pooled or recycled thread from carrying stale values — and their memory — into its
 * next use.
 */
public class FastThreadLocalThread extends Thread {

    private InternalThreadLocalMap threadLocalMap;

    /**
     * @param target the thread's body
     */
    public FastThreadLocalThread(Runnable target) {
        super(FastThreadLocalRunnable.wrap(target));
    }

    /**
     * @param group  the thread group to join
     * @param target the thread's body
     */
    public FastThreadLocalThread(ThreadGroup group, Runnable target) {
        super(group, FastThreadLocalRunnable.wrap(target));
    }

    /**
     * @param target the thread's body
     * @param name   the thread name
     */
    public FastThreadLocalThread(Runnable target, String name) {
        super(FastThreadLocalRunnable.wrap(target), name);
    }

    /**
     * @param group  the thread group to join
     * @param target the thread's body
     * @param name   the thread name
     */
    public FastThreadLocalThread(ThreadGroup group, Runnable target, String name) {
        super(group, FastThreadLocalRunnable.wrap(target), name);
    }

    /**
     * @param group     the thread group to join
     * @param target    the thread's body
     * @param name      the thread name
     * @param stackSize the desired stack size in bytes, or {@code 0} to let the JVM decide; a hint the platform
     *                  may ignore
     */
    public FastThreadLocalThread(ThreadGroup group, Runnable target, String name, long stackSize) {
        super(group, FastThreadLocalRunnable.wrap(target), name, stackSize);
    }

    final InternalThreadLocalMap threadLocalMap() {
        return threadLocalMap;
    }

    final void setThreadLocalMap(InternalThreadLocalMap threadLocalMap) {
        this.threadLocalMap = threadLocalMap;
    }

    @Override
    public void run() {
        // keep it overridden it helps in stack to see if we are using a regular thread or a FastThreadLocalThread
        super.run();
    }
}