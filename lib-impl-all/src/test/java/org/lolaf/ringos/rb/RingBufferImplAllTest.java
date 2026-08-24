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

import java.util.Comparator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * With all three builder implementations on the classpath, {@link RingBufferFactory} must select the best one
 * for the current runtime. This test adapts to the JVM it runs under; the module runs it twice (with and
 * without {@code --add-opens java.base/jdk.internal.misc=ALL-UNNAMED}) to cover both branches.
 */
class RingBufferImplAllTest {

    private static List<RingBufferBuilderProvider> providers() {
        return ServiceLoader.load(RingBufferBuilderProvider.class).stream()
                .map(ServiceLoader.Provider::get)
                .collect(Collectors.toList());
    }

    private boolean jdkInternalMiscOpen() {
        return Object.class.getModule().isOpen("jdk.internal.misc", RingBufferImplAllTest.class.getModule());
    }

    @Test
    void selectsBuilderMatchingRuntime() {
        boolean open = jdkInternalMiscOpen();
        int feature = Runtime.version().feature();

        RingBuffer<Object> ringBuffer =
                RingBufferFactory.build(RingBufferFactory.AccessType.SINGLE_CONSUMER_SINGLE_PRODUCER, 16);
        assertThat(ringBuffer).isNotNull();

        String impl = ringBuffer.getClass().getSimpleName();
        if (!open) {
            // No --add-opens: the Unsafe builders are out, leaving the MethodHandle fallback.
            assertThat(impl).startsWith("MethodHandle");
        } else if (feature >= 15) {
            assertThat(impl).startsWith("Unsafe");
        } else {
            // Java 11 to 14 with the flag: the older Unsafe builder, never the MethodHandle fallback.
            assertThat(impl).startsWith("J11Unsafe");
        }
    }

    @Test
    void picksTheSlotFlaggedSingleConsumerBufferOnlyWhenThereAreNoPooledInstances() {
        RingBuffer<Object> withoutPool =
                RingBufferFactory.build(RingBufferFactory.AccessType.SINGLE_CONSUMER_MULTI_PRODUCER, 16);
        assertThat(withoutPool.getClass().getSimpleName())
                .endsWith("MpScRingBuffer")
                .doesNotContain("Pooled");

        RingBuffer<Object> withPool = RingBufferFactory.build(
                RingBufferFactory.AccessType.SINGLE_CONSUMER_MULTI_PRODUCER, 16, i -> new Object());
        assertThat(withPool.getClass().getSimpleName()).endsWith("PooledMpScRingBuffer");
    }

    @Test
    void methodHandleProviderClaimsEveryRuntime() {
        assertThat(providers())
                .filteredOn(p -> p.getClass().getSimpleName().startsWith("MethodHandle"))
                .singleElement()
                .satisfies(p -> {
                    assertThat(p.isForCurrentRuntime()).isTrue();
                    assertThat(p.priority()).isEqualTo(RingBufferBuilderProvider.LAST_RESORT);
                });
    }

    @Test
    void anUnsafeProviderOutranksTheFallbackWhenJdkInternalMiscIsOpen() {
        List<RingBufferBuilderProvider> claiming = providers().stream()
                .filter(RingBufferBuilderProvider::isForCurrentRuntime)
                .collect(Collectors.toList());

        if (!jdkInternalMiscOpen()) {
            // Only the fallback can run here, so there is no ranking to check.
            assertThat(claiming).singleElement()
                    .satisfies(p -> assertThat(p.getClass().getSimpleName()).startsWith("MethodHandle"));
            return;
        }
        // The fallback plus exactly one Unsafe provider, and the Unsafe one must rank ahead of it.
        assertThat(claiming).hasSize(2)
                .anySatisfy(p -> assertThat(p.getClass().getSimpleName()).startsWith("MethodHandle"));
        assertThat(claiming.stream().min(Comparator.comparingInt(RingBufferBuilderProvider::priority)).orElseThrow())
                .satisfies(p -> assertThat(p.implementationClassName()).endsWith("UnsafeRbBuilder"));
    }
}
