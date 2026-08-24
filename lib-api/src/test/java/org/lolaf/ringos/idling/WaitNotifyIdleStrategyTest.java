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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

class WaitNotifyIdleStrategyTest {

    @Test
    void testWaitNotifyIdleStrategy() {
        WaitNotifyIdleStrategy s = new WaitNotifyIdleStrategy();

        new Thread(() -> {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
            s.wakeup();
        }).start();

        long startTime = System.currentTimeMillis();

        s.idle(0);

        Assertions.assertThat(System.currentTimeMillis() - startTime).isGreaterThanOrEqualTo(10);
    }

    /**
     * The wakeup lands while the consumer is still polling, before it starts waiting - the order a poll-then-idle loop
     * such as {@code AbstractRingBuffer.pollBlocking} produces whenever a producer offers in that window. The signal
     * must be kept, otherwise idle() waits for a notify that has already been sent and never returns.
     */
    @Test
    void testWakeupBeforeIdleIsNotLost() {
        WaitNotifyIdleStrategy s = new WaitNotifyIdleStrategy();

        s.wakeup();

        // preemptively, on its own thread: the whole point is that the buggy version blocks here forever
        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            s.idle();
        });
    }

    /**
     * Only one wakeup is buffered, so a second idle() with nothing new signalled must go back to waiting rather than
     * spin through on a stale flag.
     */
    @Test
    void testOnlyOneWakeupIsBuffered() throws Exception {
        WaitNotifyIdleStrategy s = new WaitNotifyIdleStrategy();
        s.wakeup();
        // preemptively as above: without the buffered signal this first idle() never returns and would hang the JVM
        // rather than fail the test
        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            s.idle();
        });

        AtomicBoolean returned = new AtomicBoolean();
        Thread idler = new Thread(() -> {
            s.idle();
            returned.set(true);
        });
        idler.start();

        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(200));
        Assertions.assertThat(returned).isFalse();

        s.wakeup();
        idler.join(TimeUnit.SECONDS.toMillis(5));
        Assertions.assertThat(returned).isTrue();
    }
}
