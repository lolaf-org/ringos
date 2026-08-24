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
 * Several producers against one consumer.
 *
 * <p>Producers contend for the tail while the consumer owns the head outright, so the single consumer still sees
 * each producer's own elements in that producer's order and with no gaps — {@link #requireConsecutive()} stays
 * on — while the order in which the producers interleave is whatever the claims came out as, and is nothing the
 * kit asserts.
 *
 * <p>This is the one access type served by two different implementations, and the variants cover both: an
 * unpooled buffer gets the slot-flagged design, which publishes through the slot itself and keeps a shared,
 * deliberately stale lower bound on the consumer's position that only a full buffer refreshes; a pooled one gets
 * the sequenced design, because a slot pre-filled with an instance can never say "nothing here". Running the
 * same load through both is what says they agree.
 */
public abstract class AbstractMpScRingBufferConcurrencyTest extends AbstractRingBufferConcurrencyTest {

    @Override
    protected AccessType accessType() {
        return AccessType.SINGLE_CONSUMER_MULTI_PRODUCER;
    }

    @Override
    protected int producerCount() {
        return severalThreads();
    }

    @Override
    protected int consumerCount() {
        return 1;
    }

    /**
     * Every producer contending for a buffer with almost no room in it: the tail claim is retried constantly and
     * the producers' shared lower bound on the consumer's position is stale nearly every time it is read.
     *
     * @param variant    the buffer shape to run against
     * @param offerStyle how the producers publish
     * @param pollStyle  how the consumer consumes
     */
    @ParameterizedTest(name = "{0}, {1} then {2}")
    @MethodSource("pacedCases")
    @Timeout(value = 120)
    void survivesEveryProducerContendingForATinyBuffer(Variant variant, OfferStyle offerStyle, PollStyle pollStyle) {
        runLoad(variant, 2, 10_000, offerStyle, pollStyle, Pacing.FLAT_OUT);
    }
}
