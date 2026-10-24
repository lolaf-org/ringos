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
 * The ringos multi-producer, single-consumer byte queue against Agrona's {@code ManyToOneRingBuffer}: a write and a
 * read on one thread, then two producers feeding one consumer. Every implementation of a benchmark is pinned to the
 * same cores, the affinity states handing them out afresh for each trial.
 *
 * @see AbstractRingBufferBenchmark for the measurement settings and the core placement
 */
public class ByteRingBufferBenchmark extends AbstractRingBufferBenchmark {

    static final int CAPACITY = 1 << 20;

    @State(Scope.Benchmark)
    public static class SingleThreadedState {
        @Param({"64", "512", "4096"})
        private int payload;

        @Param
        private ByteQueueImpl implementation;

        ByteQueueImpl.Adapter queue;

        @Setup(Level.Trial)
        public void setup() {
            queue = implementation.create(CAPACITY, payload);
        }
    }

    @State(Scope.Group)
    public static class GroupState {
        @Param({"64", "512", "4096"})
        private int payload;

        @Param({"RINGOS_MPSC", "AGRONA_MANY_TO_ONE"})
        private ByteQueueImpl implementation;

        ByteQueueImpl.Adapter queue;

        @Setup(Level.Trial)
        public void setup() {
            queue = implementation.create(CAPACITY, payload);
        }
    }

    /**
     * A write then a read on one thread: the cost of the protocol and the copy, with no contention and no handoff.
     */
    @Benchmark
    public int writeAndRead(SingleThreadedState state, ProducerThreadAffinityState affinity) {
        state.queue.write();
        return state.queue.read(1);
    }

    /**
     * Two producers contending for the tail. A write refused because the queue is full still counts as an
     * operation, so read the producers' score together with the consumer's.
     */
    @Benchmark
    @Group("TwoProducers")
    @GroupThreads(2)
    public boolean producer(GroupState state, ProducerThreadAffinityState affinity) {
        return state.queue.write();
    }

    @Benchmark
    @Group("TwoProducers")
    @GroupThreads(1)
    public int consumer(GroupState state, ConsumerThreadAffinityState affinity) {
        return state.queue.read(16);
    }

    public static void main(String[] args) throws RunnerException {
        run(ByteRingBufferBenchmark.class);
    }
}
