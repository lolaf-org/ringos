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

import net.openhft.affinity.Affinity;
import org.lolaf.ringos.threading.CpuTopology;

import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Pins a benchmark thread to a core, and judges the placement of the trial as a whole.
 */
abstract class AbstractAffinity {

    private static final Optional<CpuTopology> TOPOLOGY = CpuTopology.detect();

    /**
     * Every core pinned so far this trial, across producers and consumers alike, since the placement is
     * only worth judging once both ends of the queue are on it.
     */
    private static final Set<Integer> PINNED_CORES = new TreeSet<>();

    private static boolean placementReported;

    synchronized void setAffinity(AtomicInteger coreCounter) {
        int nextAffinityCore = coreCounter.getAndIncrement();
        Affinity.setAffinity(nextAffinityCore);
        System.out.println("Setting " + this.getClass().getSimpleName() + " affinity to core " + nextAffinityCore + " on Thread " + Thread.currentThread().getName());
        PINNED_CORES.add(nextAffinityCore);
    }

    /**
     * Warns when the pinned cores cannot measure what the benchmark means to measure: spread over
     * more than one last-level cache, every handoff crosses an interconnect and costs several times an
     * intra-die one, which swamps the differences between the implementations; doubled up on a physical
     * core, two threads meant to run at once share one core's execution units instead.
     *
     * <p>It warns rather than refuses, because either placement is a legitimate thing to measure on
     * purpose — comparing a run against a deliberately cross-die one is how the cost of crossing gets
     * quantified. It stays silent where the topology cannot be read, which is every platform without a
     * {@code sysfs} to read it from, and on the single-threaded benchmarks, where one pinned core
     * neither shares nor collides with anything.
     */
    static synchronized void reportPlacement() {
        if (placementReported || TOPOLOGY.isEmpty() || PINNED_CORES.size() < 2) {
            return;
        }
        CpuTopology topology = TOPOLOGY.get();
        boolean shared = topology.shareLastLevelCache(PINNED_CORES);
        Set<Integer> collisions = topology.smtSiblingCollisions(PINNED_CORES);
        if (!shared) {
            placementReported = true;
            System.out.println("WARNING: pinned cores " + PINNED_CORES + " do not share a last-level cache"
                    + " (this machine has " + topology.lastLevelCacheDomainCount() + " of them); every handoff"
                    + " crosses an interconnect and will dominate the measurement");
        }
        if (!collisions.isEmpty()) {
            placementReported = true;
            System.out.println("WARNING: pinned cores " + collisions + " share a physical core with another"
                    + " pinned core, so those threads compete for one core's execution units");
        }
    }

    /**
     * Forgets the trial's placement, so that a run reusing one JVM across trials judges each on its own.
     */
    static synchronized void forgetPlacement() {
        PINNED_CORES.clear();
        placementReported = false;
    }
}
