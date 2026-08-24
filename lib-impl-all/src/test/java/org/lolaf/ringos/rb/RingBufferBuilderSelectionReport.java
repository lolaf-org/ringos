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

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Prints which {@link RingBufferBuilder} the current JVM gets, and why. Not a test — a diagnostic to run by
 * hand on a given JDK, with and without {@code --add-opens java.base/jdk.internal.misc=ALL-UNNAMED}.
 */
@Slf4j
public final class RingBufferBuilderSelectionReport {

    private RingBufferBuilderSelectionReport() {
    }

    public static void main(String[] args) {
        int feature = Runtime.version().feature();
        boolean open = Object.class.getModule()
                .isOpen("jdk.internal.misc", RingBufferBuilderSelectionReport.class.getModule());

        log.info("Java feature version .......... {}", feature);
        log.info("jdk.internal.misc opened ...... {}{}", open,
                open ? "" : "   (add --add-opens java.base/jdk.internal.misc=ALL-UNNAMED to flip this)");

        List<RingBufferBuilderProvider> providers = new ArrayList<>();
        ServiceLoader.load(RingBufferBuilderProvider.class).forEach(providers::add);

        log.info("Providers on the classpath, in discovery order:");
        log.info(row("provider", "claims?", "priority", "claims?", "builder"));
        log.info(row("", "(now)", "", "(before)", ""));
        for (RingBufferBuilderProvider provider : providers) {
            String name = provider.getClass().getSimpleName();
            log.info(row(name,
                    provider.isForCurrentRuntime(),
                    priorityLabel(provider.priority()),
                    claimedBeforePriorities(name, feature, open),
                    simpleName(provider.implementationClassName())));
        }

        reportOldRule(providers, feature, open);
        reportCurrentRule();
    }

    /**
     * The rule this repository shipped before {@link RingBufferBuilderProvider#priority()} existed.
     */
    private static void reportOldRule(List<RingBufferBuilderProvider> providers, int feature, boolean open) {
        for (RingBufferBuilderProvider provider : providers) {
            if (claimedBeforePriorities(provider.getClass().getSimpleName(), feature, open)) {
                log.info("Before priorities: first claimant wins -> {}",
                        simpleName(provider.implementationClassName()));
                return;
            }
        }
        log.warn("Before priorities: NOTHING claimed this runtime, so RingBufferFactory's static initializer threw"
                        + " and the class stayed unusable for the rest of the JVM's life: IllegalStateException:"
                        + " No RingBufferBuilder implementation matches the current runtime"
                        + " (Java {}, jdk.internal.misc opened: {})", feature, open);
    }

    /**
     * The rule in force now: lowest priority among the claimants.
     */
    private static void reportCurrentRule() {
        try {
            RingBuffer<String> ringBuffer =
                    RingBufferFactory.build(RingBufferFactory.AccessType.SINGLE_CONSUMER_SINGLE_PRODUCER, 16);
            ringBuffer.offer("round-trip");
            log.info("Now:               lowest priority wins   -> {}, poll() = {}",
                    ringBuffer.getClass().getSimpleName(), ringBuffer.poll());
        } catch (Throwable t) {
            log.error("Now:               FAILED", t);
        }
    }

    private static boolean claimedBeforePriorities(String providerSimpleName, int feature, boolean open) {
        switch (providerSimpleName) {
            case "MethodHandleRingBufferBuilderProvider":
                return !open;
            case "UnsafeRingBufferBuilderProvider":
                return open && feature >= 15;
            case "J11UnsafeRingBufferBuilderProvider":
                return open && feature == 11;
            default:
                return false;
        }
    }

    /**
     * Lays one table row out to fixed column widths. The report's value is that the columns line up, and SLF4J's
     * {@code {}} placeholders cannot pad, so the row is formatted before it is handed to the logger.
     */
    private static String row(Object provider, Object claimsNow, Object priority, Object claimsBefore,
                              Object builder) {
        return String.format("  %-42s %-8s %-12s %-8s %s", provider, claimsNow, priority, claimsBefore, builder);
    }

    private static String priorityLabel(int priority) {
        if (priority == RingBufferBuilderProvider.PREFERRED) {
            return "PREFERRED";
        }
        if (priority == RingBufferBuilderProvider.LAST_RESORT) {
            return "LAST_RESORT";
        }
        return Integer.toString(priority);
    }

    private static String simpleName(String className) {
        return className.substring(className.lastIndexOf('.') + 1);
    }
}
