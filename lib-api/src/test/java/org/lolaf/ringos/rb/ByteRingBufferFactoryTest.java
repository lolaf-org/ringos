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
package org.lolaf.ringos.rb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Runs with no implementation on the classpath, which is the point: a bad capacity is refused before one is needed.
 */
class ByteRingBufferFactoryTest {

    @ParameterizedTest
    @ValueSource(ints = {0, -1024, 512, 1000, 3072})
    void aCapacityThatIsNotAPowerOfTwoOfAtLeastTheMinimumIsRefused(int capacity) {
        assertThatThrownBy(() -> ByteRingBufferFactory.queue(capacity))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Capacity must be a power of two of at least 1024 bytes, got " + capacity);
        assertThatThrownBy(() -> ByteRingBufferFactory.broadcast(capacity, ProducerType.MULTI))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(ints = {1024, 1 << 20})
    void aPowerOfTwoOfAtLeastTheMinimumIsAccepted(int capacity) {
        assertThat(ByteRingBufferFactory.checkCapacity(capacity)).isEqualTo(capacity);
    }

    @Test
    void theSelectorNamesTheByteRingServiceWhenNoneIsFound() {
        List<ByteRingBufferBuilderProvider> none = Collections.emptyList();

        assertThatThrownBy(() -> RingBufferBuilderProviderSelector.select(none, ByteRingBufferBuilderProvider.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Unable to find any ByteRingBufferBuilderProvider SPI in classpath");
    }

    @Test
    void theSelectorKeepsTheBestByteRingProvider() {
        ByteRingBufferBuilderProvider lastResort = provider(RingBufferBuilderProvider.LAST_RESORT);
        ByteRingBufferBuilderProvider preferred = provider(RingBufferBuilderProvider.PREFERRED);

        assertThat(RingBufferBuilderProviderSelector.select(List.of(lastResort, preferred), ByteRingBufferBuilderProvider.class))
                .isSameAs(preferred);
    }

    private static ByteRingBufferBuilderProvider provider(int priority) {
        return new ByteRingBufferBuilderProvider() {
            @Override
            public boolean isForCurrentRuntime() {
                return true;
            }

            @Override
            public int priority() {
                return priority;
            }

            @Override
            public String implementationClassName() {
                return "unused";
            }
        };
    }
}
