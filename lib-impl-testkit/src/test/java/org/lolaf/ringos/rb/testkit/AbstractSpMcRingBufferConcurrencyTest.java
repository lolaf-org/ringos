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
package org.lolaf.ringos.rb.testkit;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.lolaf.ringos.rb.RingBufferFactory.AccessType;

/**
 * One producer against several consumers.
 *
 * <p>The producer owns the tail outright and the consumers contend for the head, which is what changes the
 * ordering the kit may ask for. A consumer no longer sees every element, so it can no longer expect consecutive
 * sequence numbers — {@link #requireConsecutive()} goes off — but the elements it does see must still arrive in
 * the producer's order, because the head claims hand them out in the order they were published. That, plus the
 * requirement that the consumers' streams together account for every element exactly once, is the whole
 * contract: an element handed to two consumers, or to none, fails here.
 */
public abstract class AbstractSpMcRingBufferConcurrencyTest extends AbstractRingBufferConcurrencyTest {

    @Override
    protected AccessType accessType() {
        return AccessType.MULTI_CONSUMER_SINGLE_PRODUCER;
    }

    @Override
    protected int producerCount() {
        return 1;
    }

    @Override
    protected int consumerCount() {
        return severalThreads();
    }

    @Override
    protected int perProducer() {
        // a single producer feeding several consumers, so it is the producer that sets the pace
        return 100_000;
    }

    /**
     * Every consumer contending for a buffer that holds at most two elements, so the head claim is nearly always
     * a race that all but one of them loses.
     *
     * @param variant    the buffer shape to run against
     * @param offerStyle how the producer publishes
     * @param pollStyle  how the consumers consume
     */
    @ParameterizedTest(name = "{0}, {1} then {2}")
    @MethodSource("pacedCases")
    @Timeout(value = 120)
    void survivesEveryConsumerContendingForATinyBuffer(Variant variant, OfferStyle offerStyle, PollStyle pollStyle) {
        runLoad(variant, 2, 20_000, offerStyle, pollStyle, Pacing.FLAT_OUT);
    }
}
