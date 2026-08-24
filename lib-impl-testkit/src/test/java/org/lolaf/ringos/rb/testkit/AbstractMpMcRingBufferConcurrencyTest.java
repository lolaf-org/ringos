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
 * Several producers against several consumers — both ends contended, and the least that can be assumed.
 *
 * <p>Neither end is owned by one thread, so the kit asks only for what a correct implementation still owes:
 * every element reaches exactly one consumer, and the elements of any one producer reach any one consumer in the
 * order that producer published them. Nothing may be assumed about how the producers interleave, nor about which
 * consumer gets what.
 *
 * <p>Being the only access type that compares and swaps at both ends, this is where a claim protocol that lets
 * two threads win the same slot shows up — as an element delivered twice, or as one that reaches nobody because
 * its slot was overwritten before it was read.
 */
public abstract class AbstractMpMcRingBufferConcurrencyTest extends AbstractRingBufferConcurrencyTest {

    @Override
    protected AccessType accessType() {
        return AccessType.MULTI_CONSUMER_MULTI_PRODUCER;
    }

    @Override
    protected int producerCount() {
        return severalThreads();
    }

    @Override
    protected int consumerCount() {
        return severalThreads();
    }

    /**
     * Both ends contended over a buffer with almost no room in it, so producers and consumers meet at the same
     * slots constantly rather than running a lap apart.
     *
     * @param variant    the buffer shape to run against
     * @param offerStyle how the producers publish
     * @param pollStyle  how the consumers consume
     */
    @ParameterizedTest(name = "{0}, {1} then {2}")
    @MethodSource("pacedCases")
    @Timeout(value = 120)
    void survivesBothEndsContendingForATinyBuffer(Variant variant, OfferStyle offerStyle, PollStyle pollStyle) {
        runLoad(variant, 2, 10_000, offerStyle, pollStyle, Pacing.FLAT_OUT);
    }
}
