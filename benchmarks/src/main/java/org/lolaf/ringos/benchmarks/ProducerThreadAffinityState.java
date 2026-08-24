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
package org.lolaf.ringos.benchmarks;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Pins the producer threads, and the single thread of the single-threaded benchmarks.
 */
@State(Scope.Thread)
public class ProducerThreadAffinityState extends AbstractAffinity {

    /**
     * First core handed out to producer threads; each producer takes the next core upwards.
     * Overridable with {@code -Dringos.bench.producer.firstCore=<n>} to re-pin the benchmark,
     * for instance to measure the cross-die cost by placing producers on another CCD.
     */
    public static final String FIRST_CORE_PROPERTY = "ringos.bench.producer.firstCore";

    private static final int FIRST_CORE = Integer.getInteger(FIRST_CORE_PROPERTY, 9);

    private static final AtomicInteger START_CORE = new AtomicInteger(FIRST_CORE);

    @Setup(Level.Trial)
    public void setup() {
        super.setAffinity(START_CORE);
    }

    @Setup(Level.Iteration)
    public void checkPlacement() {
        reportPlacement();
    }

    @TearDown(Level.Trial)
    public void resetStartCore() {
        START_CORE.set(FIRST_CORE);
        forgetPlacement();
    }
}
