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
 * Idles with a single {@link Thread#yield()}, offering the core to another runnable thread but staying runnable
 * itself.
 *
 * <p>Sits between {@link BusySpinIdleStrategy} and {@link BackoffIdleStrategy}: it stops monopolising the core
 * against other work on the machine, yet never parks, so wake-up stays a scheduler decision away rather than a
 * timer away. On an otherwise idle core a yield returns immediately, so this still burns CPU.
 *
 * <p>Stateless, hence the shared {@link #getInstance() singleton}. It does not park, so {@link #wakeup()} is a
 * no-op for it.
 */
public class YieldingIdleStrategy implements IdleStrategy {

    private static final YieldingIdleStrategy INSTANCE = new YieldingIdleStrategy();

    /**
     * @return the shared instance; the strategy holds no state, so every caller can use the same one
     */
    public static YieldingIdleStrategy getInstance() {
        return INSTANCE;
    }

    private YieldingIdleStrategy() {
        // private
    }

    @Override
    public void idle(int workCount) {
        if (workCount > 0) {
            return;
        }
        Thread.yield();
    }

    @Override
    public void idle() {
        Thread.yield();
    }

    @Override
    public void reset() {
        // nothing to do
    }
}