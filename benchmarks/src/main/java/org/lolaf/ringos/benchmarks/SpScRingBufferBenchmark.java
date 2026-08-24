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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.RunnerException;

/**
 * Single-producer, single-consumer throughput: one pinned producer handing off to one pinned consumer,
 * plus the uncontended round-trip on a single thread.
 *
 * @see AbstractRingBufferBenchmark for the measurement settings and the core placement
 */
public class SpScRingBufferBenchmark extends AbstractRingBufferBenchmark {

    @State(Scope.Benchmark)
    public static class SingleThreadedState extends AbstractState {
        @Param({"1024"})
        private int capacity;

        /**
         * Only the single-producer, single-consumer implementations. The many-to-many baselines of
         * {@link SpScRingBufferImpl} are omitted here: with one thread there is no topology to speak of,
         * so they would build the same objects and repeat the rows of
         * {@link MpMcRingBufferBenchmark#benchmarkOfferAndPoll}.
         */
        @Param({"METHOD_HANDLE_API_SPSC", "METHOD_HANDLE_API_SPSC_PADDED",
                "UNSAFE_API_SPSC", "UNSAFE_API_SPSC_PADDED",
                "JCT_SPSC_QUEUE", "AGRONA_SPSC_QUEUE"})
        private SpScRingBufferImpl implementation;

        @Setup(Level.Trial)
        public void setup() {
            buffer = implementation.create(capacity);
        }

        @Setup(Level.Iteration)
        public void clean() {
            buffer.clear();
        }
    }

    @State(Scope.Group)
    public static class SpScState extends AbstractState {
        @Param({"4096", "8192"})
        private int capacity;

        @Param
        private SpScRingBufferImpl implementation;

        @Setup(Level.Trial)
        public void setup() {
            buffer = implementation.create(capacity);
        }
    }

    /**
     * Offer and poll on one thread, so the queue is measured with neither contention nor a cache-line
     * handoff — the floor its concurrent scores are read against.
     */
    @Benchmark
    public Object benchmarkOfferAndPoll(SingleThreadedState state,
                                        ProducerThreadAffinityState producerThreadAffinityState) {
        state.offer();
        return state.poll();
    }

    @Benchmark
    @Group("SpSc")
    @GroupThreads(1)
    public boolean producerSPSC(SpScState state, ProducerThreadAffinityState producerThreadAffinityState) {
        return state.offer();
    }

    @Benchmark
    @Group("SpSc")
    @GroupThreads(1)
    public Object consumerSPSC(SpScState state, ConsumerThreadAffinityState consumerThreadAffinityState) {
        return state.poll();
    }

    public static void main(String[] args) throws RunnerException {
        run(SpScRingBufferBenchmark.class);
    }
}
