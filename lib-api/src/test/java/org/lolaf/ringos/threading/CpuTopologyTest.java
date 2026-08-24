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
package org.lolaf.ringos.threading;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class CpuTopologyTest {

    @TempDir
    Path sysfs;

    /**
     * A two-die part in the shape of the machine these ring buffers are benchmarked on: twelve cores over two
     * dies that share no cache, each core carrying two SMT siblings numbered a half-machine apart, and a
     * larger last-level cache on the first die than on the second.
     */
    @BeforeEach
    void writeTwoDieMachine() throws IOException {
        for (int cpu = 0; cpu < 24; cpu++) {
            int core = cpu % 12;
            boolean firstDie = core < 6;
            String siblings = Math.min(core, core + 12) + "," + (core + 12);
            String shared = firstDie ? "0-5,12-17" : "6-11,18-23";
            writeCpu(cpu, siblings, shared, firstDie ? "98304K" : "32768K");
        }
    }

    private void writeCpu(int cpu, String threadSiblings, String cacheShared, String cacheSize) throws IOException {
        Path root = sysfs.resolve("cpu" + cpu);
        write(root.resolve("topology/thread_siblings_list"), threadSiblings);
        write(root.resolve("cache/index0/level"), "1");
        write(root.resolve("cache/index0/shared_cpu_list"), threadSiblings);
        write(root.resolve("cache/index0/size"), "32K");
        write(root.resolve("cache/index3/level"), "3");
        write(root.resolve("cache/index3/shared_cpu_list"), cacheShared);
        write(root.resolve("cache/index3/size"), cacheSize);
    }

    private void write(Path file, String content) throws IOException {
        Files.createDirectories(file.getParent());
        Files.write(file, content.getBytes(StandardCharsets.US_ASCII));
    }

    private CpuTopology topology() {
        Optional<CpuTopology> read = CpuTopology.read(sysfs);
        assertThat(read).isPresent();
        return read.get();
    }

    @Test
    void readsEveryCpu() {
        assertThat(topology().cpus()).hasSize(24).contains(0, 11, 23);
    }

    @Test
    void groupsCpusByTheCacheTheyShare() {
        CpuTopology topology = topology();
        assertThat(topology.lastLevelCacheDomainCount()).isEqualTo(2);
        assertThat(topology.lastLevelCacheSiblingsOf(0)).containsExactly(0, 1, 2, 3, 4, 5, 12, 13, 14, 15, 16, 17);
        assertThat(topology.lastLevelCacheSiblingsOf(6)).containsExactly(6, 7, 8, 9, 10, 11, 18, 19, 20, 21, 22, 23);
    }

    @Test
    void namesEachCacheAfterItsLowestCpu() {
        CpuTopology topology = topology();
        assertThat(topology.lastLevelCacheDomainOf(5)).hasValue(0);
        assertThat(topology.lastLevelCacheDomainOf(11)).hasValue(6);
        assertThat(topology.lastLevelCacheDomainOf(0)).isEqualTo(topology.lastLevelCacheDomainOf(17));
        assertThat(topology.lastLevelCacheDomainOf(5)).isNotEqualTo(topology.lastLevelCacheDomainOf(6));
    }

    @Test
    void detectsCpusSpanningTwoCaches() {
        CpuTopology topology = topology();
        assertThat(topology.shareLastLevelCache(Arrays.asList(6, 7, 8, 9))).isTrue();
        assertThat(topology.shareLastLevelCache(Arrays.asList(2, 3, 4, 5))).isTrue();
        assertThat(topology.shareLastLevelCache(Arrays.asList(2, 3, 6, 7))).isFalse();
    }

    @Test
    void treatsFewerThanTwoCpusAsSharing() {
        CpuTopology topology = topology();
        assertThat(topology.shareLastLevelCache(Collections.emptyList())).isTrue();
        assertThat(topology.shareLastLevelCache(Collections.singletonList(3))).isTrue();
    }

    @Test
    void refusesToVouchForAnUnknownCpu() {
        assertThat(topology().shareLastLevelCache(Arrays.asList(6, 7, 999))).isFalse();
    }

    @Test
    void reportsTheSizeOfEachCache() {
        CpuTopology topology = topology();
        assertThat(topology.lastLevelCacheBytesOf(0)).hasValue(98304L * 1024L);
        assertThat(topology.lastLevelCacheBytesOf(6)).hasValue(32768L * 1024L);
        assertThat(topology.lastLevelCacheBytesOf(999)).isEmpty();
    }

    @Test
    void findsThreadsPlacedOnOneCore() {
        CpuTopology topology = topology();
        assertThat(topology.smtSiblingsOf(6)).containsExactly(6, 18);
        assertThat(topology.smtSiblingCollisions(Arrays.asList(6, 7, 8))).isEmpty();
        assertThat(topology.smtSiblingCollisions(Arrays.asList(6, 18, 7))).containsExactly(6, 18);
    }

    @Test
    void yieldsNothingWhereThereIsNoSysfs() {
        assertThat(CpuTopology.read(sysfs.resolve("absent"))).isEmpty();
    }

    @Test
    void yieldsNothingWhereSysfsHoldsNoCpu() throws IOException {
        Path empty = sysfs.resolve("empty");
        Files.createDirectories(empty.resolve("kernel_max"));
        assertThat(CpuTopology.read(empty)).isEmpty();
    }

    @Test
    void survivesACpuWhoseFilesAreMissing() throws IOException {
        Files.createDirectories(sysfs.resolve("cpu99"));
        CpuTopology topology = topology();
        assertThat(topology.lastLevelCacheDomainOf(99)).isEmpty();
        assertThat(topology.smtSiblingsOf(99)).isEmpty();
        assertThat(topology.lastLevelCacheDomainCount()).isEqualTo(2);
    }

    @Test
    void parsesTheCpuListsSysfsWrites() throws IOException {
        Path file = sysfs.resolve("list");
        write(file, "0-3,8,10-11");
        assertThat(CpuTopology.readCpuList(file)).hasValue(new java.util.TreeSet<>(Arrays.asList(0, 1, 2, 3, 8, 10, 11)));
    }
}
