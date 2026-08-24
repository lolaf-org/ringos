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
 * One producer against one consumer.
 *
 * <p>This is the access type with the strongest guarantee, and the kit asks for all of it: with nobody else
 * publishing and nobody else consuming, the consumer must see exactly the sequence the producer published, in
 * order and with no gaps — which is what {@link #requireConsecutive()} turns on.
 *
 * <p>It is also the access type with the least machinery to get there. Neither end compares and swaps, and each
 * side keeps a cached view of the other's position rather than reading it every time, so the failure to look for
 * is a cache that goes stale and is never refreshed. That is why the runs here are deliberately long and the
 * buffers deliberately small: a stale bound only bites once the buffer has actually reached the end the cache
 * was wrong about.
 */
public abstract class AbstractSpScRingBufferConcurrencyTest extends AbstractRingBufferConcurrencyTest {

    @Override
    protected AccessType accessType() {
        return AccessType.SINGLE_CONSUMER_SINGLE_PRODUCER;
    }

    @Override
    protected int producerCount() {
        return 1;
    }

    @Override
    protected int consumerCount() {
        return 1;
    }

    @Override
    protected int perProducer() {
        // a single producer thread, so the same element count costs a quarter of what the multi-producer
        // classes pay for it
        return 100_000;
    }

    /**
     * A capacity of two leaves the buffer either empty or full at almost every moment, so each side's cached
     * view of the other is wrong nearly every time it is consulted and has to be refreshed.
     *
     * @param variant    the buffer shape to run against
     * @param offerStyle how the producer publishes
     * @param pollStyle  how the consumer consumes
     */
    @ParameterizedTest(name = "{0}, {1} then {2}")
    @MethodSource("pacedCases")
    @Timeout(value = 120)
    void survivesABufferThatIsOnlyEverOneOrTwoDeep(Variant variant, OfferStyle offerStyle, PollStyle pollStyle) {
        runLoad(variant, 2, 20_000, offerStyle, pollStyle, Pacing.FLAT_OUT);
    }
}
