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
 * Never gives the core up: idling is a single {@link Thread#onSpinWait()}, so the thread stays hot and reacts to
 * a publication in the tens of nanoseconds.
 *
 * <p>The right choice only when the polling thread owns its core and the latency is worth the burn — it consumes
 * 100% of that core whether or not there is work. {@link BackoffIdleStrategy} is the usual answer when it isn't.
 *
 * <p>Stateless, hence the shared {@link #getInstance() singleton}: unlike a backing-off strategy, one instance
 * can serve any number of idling threads. It does not park, so {@link #wakeup()} is a no-op for it.
 */
public class BusySpinIdleStrategy implements IdleStrategy {

    private static final BusySpinIdleStrategy INSTANCE = new BusySpinIdleStrategy();

    /**
     * @return the shared instance; the strategy holds no state, so every caller can use the same one
     */
    public static BusySpinIdleStrategy getInstance() {
        return INSTANCE;
    }

    private BusySpinIdleStrategy() {

    }

    @Override
    public void idle(int workCount) {
        if (workCount > 0) {
            return;
        }
        Thread.onSpinWait();
    }

    @Override
    public void idle() {
        Thread.onSpinWait();
    }

    @Override
    public void reset() {
        // nothing to do
    }
}