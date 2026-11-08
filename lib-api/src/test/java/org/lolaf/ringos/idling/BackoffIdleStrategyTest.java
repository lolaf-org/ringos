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

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class BackoffIdleStrategyTest {

    private static final long PARK_NANOS = TimeUnit.SECONDS.toNanos(10);

    @Test
    void aWakeupCutsAParkShort() throws InterruptedException {
        BackoffIdleStrategy idle = new BackoffIdleStrategy(0, 0, PARK_NANOS, PARK_NANOS);
        idle.idle();
        idle.idle();
        long[] idledNanos = new long[1];
        Thread idler = new Thread(() -> {
            long start = System.nanoTime();
            idle.idle();
            idledNanos[0] = System.nanoTime() - start;
        });
        idler.start();
        while (idler.isAlive() && idler.getState() != Thread.State.TIMED_WAITING) {
            Thread.onSpinWait();
        }

        idle.wakeup();
        idler.join(TimeUnit.NANOSECONDS.toMillis(PARK_NANOS) / 2);

        assertThat(idler.isAlive()).isFalse();
        assertThat(idledNanos[0]).isLessThan(PARK_NANOS / 2);
    }

    @Test
    void aWakeupWithNoThreadParkedDoesNothing() {
        BackoffIdleStrategy idle = new BackoffIdleStrategy();

        idle.wakeup();
        idle.idle();
    }
}
