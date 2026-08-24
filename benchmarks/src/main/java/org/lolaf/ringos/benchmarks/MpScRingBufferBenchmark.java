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
 * Multi-producer, single-consumer throughput: two pinned producers feeding one pinned consumer, plus
 * the uncontended round-trip on a single thread.
 *
 * @see AbstractRingBufferBenchmark for the measurement settings and the core placement
 */
public class MpScRingBufferBenchmark extends AbstractRingBufferBenchmark {

    @State(Scope.Benchmark)
    public static class SingleThreadedState extends AbstractState {
        @Param({"1024"})
        private int capacity;

        /**
         * Only the single-consumer implementations. The many-to-many baselines of
         * {@link MpScRingBufferImpl} are omitted here: with one thread there is no topology to speak of,
         * so they would build the same objects and repeat the rows of
         * {@link MpMcRingBufferBenchmark#benchmarkOfferAndPoll}.
         */
        @Param({"METHOD_HANDLE_API_MPSC", "METHOD_HANDLE_API_MPSC_PADDED",
                "UNSAFE_API_MPSC", "UNSAFE_API_MPSC_PADDED",
                "METHOD_HANDLE_API_MPSC_SEQUENCED", "UNSAFE_API_MPSC_SEQUENCED",
                "JCT_MPSC_QUEUE", "AGRONA_MPSC_QUEUE"})
        private MpScRingBufferImpl implementation;

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
    public static class MpScState extends AbstractState {
        @Param({"4096", "8192"})
        private int capacity;

        @Param
        private MpScRingBufferImpl implementation;

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

    /**
     * Two producers feeding a single consumer.
     *
     * <p>The consumer keeps up: {@link AbstractState#poll()} spins inside a single operation until an
     * element arrives, so the group settles into lockstep where every offer is matched by a poll. The
     * consumer therefore completes exactly twice the operations of each producer, and its score is
     * pinned at half the producer score by construction rather than measuring anything on its own.
     * Read the producer score, or the aggregate.
     */
    @Benchmark
    @Group("TwoProducerMpSc")
    @GroupThreads(2)
    public boolean producerMpSc(MpScState state, ProducerThreadAffinityState producerThreadAffinityState) {
        return state.offer();
    }

    @Benchmark
    @Group("TwoProducerMpSc")
    @GroupThreads(1)
    public Object consumerMpSc(MpScState state, ConsumerThreadAffinityState consumerThreadAffinityState) {
        return state.poll();
    }

    public static void main(String[] args) throws RunnerException {
        run(MpScRingBufferBenchmark.class);
    }
}
