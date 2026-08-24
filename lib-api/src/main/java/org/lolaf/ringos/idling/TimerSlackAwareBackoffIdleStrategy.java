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

import lombok.extern.slf4j.Slf4j;
import org.lolaf.ringos.clib.CLibrary;
import org.lolaf.ringos.clib.CLibraryApi;

import java.time.Duration;

/**
 * A {@link BackoffIdleStrategy} whose park periods below 50 microseconds are actually honoured.
 *
 * <p>Linux rounds a park up by the calling thread's timer slack, 50 microseconds by default, so the plain backoff
 * strategy cannot park for less than that however small its {@code minParkPeriodNs}. This subclass narrows the
 * slack to that period on the thread that idles on it, through a native {@code prctl} call in
 * {@link #assignToThread(Thread)}.
 *
 * <p>Worth its cost only where a park of a few microseconds is the point; otherwise use
 * {@link BackoffIdleStrategy}. The narrowed slack applies to the whole thread, not just to this strategy's parks,
 * and stays in force for as long as the thread lives — so give it a thread that is yours to configure.
 *
 * <p>Like its parent, one instance serves one idling thread.
 */
@Slf4j
public class TimerSlackAwareBackoffIdleStrategy extends BackoffIdleStrategy {

    /**
     * Builds a strategy on the {@code DEFAULT_} constants of {@link BackoffIdleStrategy}, whose 50 microsecond
     * minimum park period already matches the default timer slack — so this constructor buys nothing over the
     * parent unless the running thread's slack has been widened elsewhere.
     */
    public TimerSlackAwareBackoffIdleStrategy() {
        super();
    }

    /**
     * Builds a strategy with an explicit escalation profile, narrowing the idling thread's timer slack to
     * {@code minParkPeriodNs} so a sub-50-microsecond park is not rounded back up.
     *
     * @param maxSpins        how many {@link Thread#onSpinWait()} idles to do before yielding
     * @param maxYields       how many {@link Thread#yield()} idles to do before parking
     * @param minParkPeriodNs the first park period, in nanoseconds, and the timer slack applied to the idling
     *                        thread
     * @param maxParkPeriodNs the ceiling the park period doubles up to, in nanoseconds
     */
    public TimerSlackAwareBackoffIdleStrategy(
            final long maxSpins, final long maxYields, final long minParkPeriodNs, final long maxParkPeriodNs) {
        super(maxSpins, maxYields, minParkPeriodNs, maxParkPeriodNs);
    }

    @Override
    public void assignToThread(Thread thread) {
        CLibrary clib = CLibraryApi.get();
        long currentThreadSlack = clib.getTimerSlack().toNanos();
        if (currentThreadSlack != 0 && currentThreadSlack > getMinParkPeriodNs()) {
            log.info("Setting thread {} timer slack to {} nanos", thread.getName(), getMinParkPeriodNs());
            clib.setTimerSlack(Duration.ofNanos(getMinParkPeriodNs()));
        }
    }

}