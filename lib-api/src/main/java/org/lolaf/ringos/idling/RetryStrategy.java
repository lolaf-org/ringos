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

import java.util.function.BooleanSupplier;

/**
 * Waits between the attempts of an offer refused by a full ring buffer, and decides when to give up, so a
 * producer can stop waiting on a consumer that is gone.
 *
 * <p>Like {@link IdleStrategy}, an instance carries the backoff state of the one thread offering through it: give
 * each offering thread its own.
 */
public interface RetryStrategy {

    /**
     * Called once per offer, when the first attempt fails and before the first {@link #awaitRetry()}; never
     * called when there is room straight away.
     */
    void reset();

    /**
     * Called after each failed attempt: waits, then says whether to try again.
     *
     * @return {@code true} to retry, {@code false} to give up and make the offer return {@code false}
     */
    boolean awaitRetry();

    /**
     * Idles on {@code idleStrategy} between attempts, for as long as {@code keepTrying} holds. Allocates the
     * returned instance, so keep it in a field rather than calling this per offer.
     *
     * @param idleStrategy how to wait between attempts; owned by the returned strategy from now on
     * @param keepTrying   checked after each wait; {@code false} gives up
     * @return a strategy delegating {@link #reset()} to {@code idleStrategy}
     */
    static RetryStrategy idlingWhile(IdleStrategy idleStrategy, BooleanSupplier keepTrying) {
        return new RetryStrategy() {
            @Override
            public void reset() {
                idleStrategy.reset();
            }

            @Override
            public boolean awaitRetry() {
                idleStrategy.idle();
                return keepTrying.getAsBoolean();
            }
        };
    }
}
