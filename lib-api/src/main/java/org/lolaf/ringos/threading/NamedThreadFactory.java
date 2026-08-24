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

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link ThreadFactory} producing threads named {@code prefix-n}, numbered from 1 in creation order, so that
 * a thread dump or a profiler attributes work to the component that spawned it.
 *
 * <p>Its threads differ from {@link java.util.concurrent.Executors#defaultThreadFactory()} in two further ways:
 * they are daemons by default, and each carries an uncaught-exception handler that logs the failure — without
 * which a thread dying in a polling loop would leave nothing behind but a stalled queue.
 *
 * <p>The threads are plain {@link Thread}s; {@link FastThreadLocal} users need
 * {@link FastThreadLocalThread} instead.
 */
@Slf4j
public class NamedThreadFactory implements ThreadFactory {

    private final AtomicInteger sequence = new AtomicInteger(1);

    private final String prefix;
    private final boolean daemon;

    /**
     * Builds a factory producing daemon threads.
     *
     * @param prefix the name prefix, to which {@code -n} is appended per thread
     */
    public NamedThreadFactory(String prefix) {
        this(prefix, true);
    }

    /**
     * @param prefix the name prefix, to which {@code -n} is appended per thread
     * @param daemon whether the threads are daemons, i.e. whether the JVM may exit while they still run; pass
     *               {@code false} for a thread whose loop must keep the JVM alive
     */
    public NamedThreadFactory(String prefix, boolean daemon) {
        this.prefix = prefix;
        this.daemon = daemon;
    }

    @Override
    /**
     * Builds the next thread: named from the prefix and this factory's counter, daemon as configured, and given
     * an uncaught-exception handler that logs at error level.
     * <p>
     * The thread is returned unstarted, as the {@link ThreadFactory} contract requires.
     *
     * @param runnable the thread's body
     * @return the new thread
     */
    public Thread newThread(Runnable runnable) {
        Thread thread = new Thread(runnable, prefix + "-" + sequence.getAndIncrement());
        thread.setDaemon(daemon);
        thread.setUncaughtExceptionHandler((t, e) -> log.error("Uncaught exception in thread {}", t.getName(), e));
        return thread;
    }

}
