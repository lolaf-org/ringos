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

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * Picks the {@link RingBufferBuilderProvider} to use out of the ones on the classpath. Kept apart from
 * {@link RingBufferFactory} so the rule can be exercised against hand-made providers without initialising the
 * factory, whose static initializer performs the real {@link java.util.ServiceLoader} lookup.
 */
@Slf4j
@UtilityClass
class RingBufferBuilderProviderSelector {

    /**
     * Returns the eligible provider with the lowest {@link RingBufferBuilderProvider#priority()}. Providers are
     * only ever asked {@link RingBufferBuilderProvider#isForCurrentRuntime()} and
     * {@link RingBufferBuilderProvider#priority()} here — no builder class is loaded, so an implementation the
     * runtime cannot support is never linked.
     *
     * @param candidates every provider found on the classpath, in discovery order
     * @return the provider whose builder should be used
     * @throws IllegalStateException if {@code candidates} is empty, or if none of them claims this runtime
     */
    static RingBufferBuilderProvider select(Iterable<RingBufferBuilderProvider> candidates) {
        List<String> seen = new ArrayList<>();
        List<String> eligible = new ArrayList<>();
        RingBufferBuilderProvider chosen = null;
        for (RingBufferBuilderProvider candidate : candidates) {
            seen.add(candidate.getClass().getName());
            if (!candidate.isForCurrentRuntime()) {
                continue;
            }
            eligible.add(candidate.getClass().getName());
            // Strictly lower, so the first provider of a given priority keeps it and the outcome does not
            // depend on ServiceLoader's iteration order any more than it has to.
            if (chosen == null || candidate.priority() < chosen.priority()) {
                chosen = candidate;
            }
        }
        if (seen.isEmpty()) {
            throw new IllegalStateException("Unable to find any RingBufferBuilderProvider SPI in classpath");
        }
        if (chosen == null) {
            throw new IllegalStateException("No RingBufferBuilder implementation matches the current runtime "
                    + "(Java " + Runtime.version().feature() + ", jdk.internal.misc opened: "
                    + Object.class.getModule().isOpen("jdk.internal.misc", RingBufferBuilderProviderSelector.class.getModule())
                    + "); candidates were " + seen);
        }
        if (eligible.size() > 1) {
            log.debug("RingBufferBuilderProviders claiming this runtime: {}; kept {} on priority", eligible,
                    chosen.getClass().getName());
        }
        return chosen;
    }
}
