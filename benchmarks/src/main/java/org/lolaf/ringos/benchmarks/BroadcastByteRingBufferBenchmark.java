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
 * The ringos broadcast byte ring buffers against Agrona's {@code BroadcastTransmitter}: a write and a read on one
 * thread, one writer with one reader, and two writers with one reader. Every implementation of a benchmark is pinned
 * to the same cores, the affinity states handing them out afresh for each trial.
 *
 * <p>The readers never wait for a writer: a reader that finds nothing returns at once, and one that is lapped jumps to
 * the tail. Read the writers' score; the reader's says how often it found a frame.
 *
 * @see AbstractRingBufferBenchmark for the measurement settings and the core placement
 */
public class BroadcastByteRingBufferBenchmark extends AbstractRingBufferBenchmark {

    static final int CAPACITY = 1 << 20;

    @State(Scope.Benchmark)
    public static class SingleThreadedState {
        @Param({"64", "512", "4096"})
        private int payload;

        @Param({"RINGOS_SINGLE", "RINGOS_SINGLE_JAVA11", "RINGOS_MULTI", "AGRONA"})
        private BroadcastImpl implementation;

        BroadcastImpl.Adapter ring;

        @Setup(Level.Trial)
        public void setup() {
            ring = implementation.create(CAPACITY, payload);
        }
    }

    @State(Scope.Group)
    public static class OneWriterState {
        @Param({"64", "512", "4096"})
        private int payload;

        @Param({"RINGOS_SINGLE", "AGRONA"})
        private BroadcastImpl implementation;

        BroadcastImpl.Adapter ring;

        @Setup(Level.Trial)
        public void setup() {
            ring = implementation.create(CAPACITY, payload);
        }
    }

    @State(Scope.Group)
    public static class TwoWritersState {
        @Param({"64", "512", "4096"})
        private int payload;

        @Param({"RINGOS_MULTI", "AGRONA_SPIN_LOCKED"})
        private BroadcastImpl implementation;

        BroadcastImpl.Adapter ring;

        @Setup(Level.Trial)
        public void setup() {
            ring = implementation.create(CAPACITY, payload);
        }
    }

    /**
     * A write then a read on one thread: the cost of the protocol and both copies, uncontended.
     */
    @Benchmark
    public int writeAndRead(SingleThreadedState state, ProducerThreadAffinityState affinity) {
        state.ring.write();
        return state.ring.read();
    }

    @Benchmark
    @Group("OneWriter")
    @GroupThreads(1)
    public void oneWriter(OneWriterState state, ProducerThreadAffinityState affinity) {
        state.ring.write();
    }

    @Benchmark
    @Group("OneWriter")
    @GroupThreads(1)
    public int oneWriterReader(OneWriterState state, ConsumerThreadAffinityState affinity) {
        return state.ring.read();
    }

    /**
     * Two writers serialized by a spin lock, ringos' own and the same one around Agrona's single writer.
     */
    @Benchmark
    @Group("TwoWriters")
    @GroupThreads(2)
    public void twoWriters(TwoWritersState state, ProducerThreadAffinityState affinity) {
        state.ring.write();
    }

    @Benchmark
    @Group("TwoWriters")
    @GroupThreads(1)
    public int twoWritersReader(TwoWritersState state, ConsumerThreadAffinityState affinity) {
        return state.ring.read();
    }

    public static void main(String[] args) throws RunnerException {
        run(BroadcastByteRingBufferBenchmark.class);
    }
}
