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
 * Parks the idling thread on {@code Object.wait} until {@link #wakeup()} is called. One instance is meant to serve a
 * single idling thread, the producers calling {@link #wakeup()} being any number of others.
 */
public class WaitNotifyIdleStrategy implements IdleStrategy {

    /**
     * Raised by {@link #wakeup()} and consumed by {@link #idle()}, so that a wakeup published while the consumer had
     * seen its queue empty but had not started waiting yet is not lost.
     * <p>
     * Callers reach {@link #idle()} through a poll-then-idle loop, {@code AbstractRingBuffer.pollBlocking} being the
     * one in this library: it polls, finds nothing, and only then idles. A producer that offers in between publishes
     * its {@code notify()} to nobody, and without this flag the consumer goes on to an unbounded {@code wait()} it is
     * never woken from - asleep on a queue that has work in it. The window is a few instructions wide, so it takes a
     * loaded machine to lose that race, and losing it strands the consumer for good.
     * <p>
     * Guarded by this instance's monitor.
     */
    private boolean signalled;

    @Override
    public void idle(int workCount) {
        if (workCount == 0) {
            idle();
        }
    }

    @Override
    public void idle() {
        synchronized (this) {
            if (signalled) {
                // woken before we got here, the caller must go back to polling rather than wait for the next signal
                signalled = false;
                return;
            }
            try {
                this.wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                signalled = false;
            }
        }
    }

    @Override
    public void reset() {
        // deliberately does not clear the flag: reset() is called by the poll-then-idle loop before it starts idling,
        // so clearing here would drop exactly the wakeups the flag exists to keep
    }

    @Override
    public void wakeup() {
        synchronized (this) {
            signalled = true;
            this.notify();
        }
    }
}