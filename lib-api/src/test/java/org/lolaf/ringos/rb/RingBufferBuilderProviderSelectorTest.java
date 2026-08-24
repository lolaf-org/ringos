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

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Selection rule on its own, against hand-made providers. The bundled providers can only be observed on the JDK
 * the build happens to run — this pins the ordering itself, including the case the real classpath cannot
 * produce today (two eligible providers at the same priority).
 */
class RingBufferBuilderProviderSelectorTest {

    /**
     * A provider that claims (or not) the runtime at a given priority, and blows up if anyone asks for its
     * builder — so the test also proves the loser's implementation class is never even named.
     */
    private static class FakeProvider implements RingBufferBuilderProvider {

        private final String name;
        private final boolean eligible;
        private final int priority;

        FakeProvider(String name, boolean eligible, int priority) {
            this.name = name;
            this.eligible = eligible;
            this.priority = priority;
        }

        @Override
        public boolean isForCurrentRuntime() {
            return eligible;
        }

        @Override
        public int priority() {
            return priority;
        }

        @Override
        public String implementationClassName() {
            throw new AssertionError(name + " was asked for its builder");
        }

        @Override
        public String toString() {
            return name;
        }
    }

    private static FakeProvider eligible(String name, int priority) {
        return new FakeProvider(name, true, priority);
    }

    private static FakeProvider ineligible(String name) {
        return new FakeProvider(name, false, RingBufferBuilderProvider.PREFERRED);
    }

    @Test
    void picksTheLowestPriorityAmongTheEligible() {
        FakeProvider preferred = eligible("preferred", RingBufferBuilderProvider.PREFERRED);
        FakeProvider lastResort = eligible("lastResort", RingBufferBuilderProvider.LAST_RESORT);

        assertThat(RingBufferBuilderProviderSelector.select(List.of(preferred, lastResort))).isSameAs(preferred);
        // ...and the discovery order must not matter
        assertThat(RingBufferBuilderProviderSelector.select(List.of(lastResort, preferred))).isSameAs(preferred);
    }

    @Test
    void fallsBackToTheLastResortWhenItIsTheOnlyClaimant() {
        FakeProvider lastResort = eligible("lastResort", RingBufferBuilderProvider.LAST_RESORT);

        assertThat(RingBufferBuilderProviderSelector.select(List.of(ineligible("unsafe"), lastResort)))
                .isSameAs(lastResort);
    }

    @Test
    void ignoresThePriorityOfProvidersThatDoNotClaimTheRuntime() {
        FakeProvider lastResort = eligible("lastResort", RingBufferBuilderProvider.LAST_RESORT);

        // The ineligible one sits at PREFERRED; it must not win, nor be asked for its builder.
        assertThat(RingBufferBuilderProviderSelector.select(List.of(ineligible("betterButUnusable"), lastResort)))
                .isSameAs(lastResort);
    }

    @Test
    void keepsDiscoveryOrderBetweenEqualPriorities() {
        FakeProvider first = eligible("first", RingBufferBuilderProvider.PREFERRED);
        FakeProvider second = eligible("second", RingBufferBuilderProvider.PREFERRED);

        assertThat(RingBufferBuilderProviderSelector.select(List.of(first, second))).isSameAs(first);
        assertThat(RingBufferBuilderProviderSelector.select(List.of(second, first))).isSameAs(second);
    }

    @Test
    void failsWhenNoProviderIsOnTheClasspath() {
        assertThatThrownBy(() -> RingBufferBuilderProviderSelector.select(Collections.emptyList()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unable to find any RingBufferBuilderProvider SPI");
    }

    @Test
    void failsNamingTheCandidatesWhenNoneClaimsTheRuntime() {
        assertThatThrownBy(() -> RingBufferBuilderProviderSelector.select(List.of(ineligible("a"), ineligible("b"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No RingBufferBuilder implementation matches the current runtime")
                .hasMessageContaining(FakeProvider.class.getName());
    }

    @Test
    void defaultPriorityIsPreferred() {
        RingBufferBuilderProvider defaulted = new RingBufferBuilderProvider() {
            @Override
            public boolean isForCurrentRuntime() {
                return true;
            }

            @Override
            public String implementationClassName() {
                return "unused";
            }
        };

        assertThat(defaulted.priority()).isEqualTo(RingBufferBuilderProvider.PREFERRED);
    }
}
