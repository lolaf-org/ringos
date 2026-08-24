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

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Which CPUs share a last-level cache, and which share a physical core, read from {@code sysfs}.
 *
 * <p>It answers the one question that decides where a producer and a consumer should run: do they share a
 * cache, or does every handoff between them have to cross an interconnect? On a chiplet CPU the cores of one
 * socket are split across dies that share nothing below main memory, and a queue whose two ends land on
 * different dies pays several times the cost of one whose ends share a last-level cache — without anything in
 * the JVM, the thread names or the affinity call giving a hint that it happened.
 *
 * <p>The obvious ways to look for that split do not find it. NUMA typically reports a single node for such a
 * socket, {@code topology/cluster_id} is frequently unset, and {@code topology/die_id} is only as reliable as
 * the vendor's ACPI tables. What holds across machines is the cache hierarchy itself: two CPUs listed in each
 * other's deepest {@code cache/indexN/shared_cpu_list} share that cache, and two that are not, do not. This
 * class reads exactly that, plus {@code topology/thread_siblings_list} for the SMT pairing.
 *
 * <p>It reports what {@code sysfs} exposes, which is the <b>host's</b> layout — inside a container that is not
 * the set of CPUs the process may actually use, so intersect it with the real affinity mask before drawing
 * conclusions. Everything degrades to an empty result rather than a wrong one: {@link #detect()} yields an
 * empty {@link Optional} wherever {@code sysfs} is absent or unreadable, and the per-CPU queries yield empty
 * for a CPU whose files could not be parsed. Nothing here is cached, and nothing here is on a hot path.
 */
@Slf4j
public final class CpuTopology {

    private static final Path DEFAULT_SYSFS_CPU_ROOT = Paths.get("/sys/devices/system/cpu");

    private static final Pattern CPU_DIRECTORY = Pattern.compile("cpu\\d+");

    private final Map<Integer, Set<Integer>> smtSiblings;

    private final Map<Integer, Set<Integer>> lastLevelCacheSiblings;

    private final Map<Integer, Long> lastLevelCacheBytes;

    private CpuTopology(Map<Integer, Set<Integer>> smtSiblings,
                        Map<Integer, Set<Integer>> lastLevelCacheSiblings,
                        Map<Integer, Long> lastLevelCacheBytes) {
        this.smtSiblings = smtSiblings;
        this.lastLevelCacheSiblings = lastLevelCacheSiblings;
        this.lastLevelCacheBytes = lastLevelCacheBytes;
    }

    /**
     * Reads the topology of the running machine.
     *
     * @return the topology, or empty on a platform with no {@code /sys/devices/system/cpu} to read — every
     * non-Linux one, and a Linux one whose {@code sysfs} is not mounted or is masked
     */
    public static Optional<CpuTopology> detect() {
        return read(DEFAULT_SYSFS_CPU_ROOT);
    }

    /**
     * Reads the topology rooted at an arbitrary directory laid out like {@code /sys/devices/system/cpu}, which
     * is what makes this class testable without the machine having the shape the test needs.
     *
     * @param sysfsCpuRoot the directory holding the {@code cpuN} entries
     * @return the topology, or empty when the directory does not exist or holds no {@code cpuN} entry
     */
    static Optional<CpuTopology> read(Path sysfsCpuRoot) {
        if (!Files.isDirectory(sysfsCpuRoot)) {
            log.debug("No CPU topology to read: {} is not a directory", sysfsCpuRoot);
            return Optional.empty();
        }

        Map<Integer, Set<Integer>> smt = new TreeMap<>();
        Map<Integer, Set<Integer>> llc = new TreeMap<>();
        Map<Integer, Long> llcBytes = new TreeMap<>();

        try (DirectoryStream<Path> entries = Files.newDirectoryStream(sysfsCpuRoot)) {
            for (Path entry : entries) {
                String name = entry.getFileName().toString();
                if (!CPU_DIRECTORY.matcher(name).matches() || !Files.isDirectory(entry)) {
                    continue;
                }
                int cpu = Integer.parseInt(name.substring("cpu".length()));
                readCpuList(entry.resolve("topology/thread_siblings_list"))
                        .ifPresent(siblings -> smt.put(cpu, siblings));
                deepestCache(entry.resolve("cache")).ifPresent(cache -> {
                    llc.put(cpu, cache.sharedCpus);
                    if (cache.sizeBytes > 0L) {
                        llcBytes.put(cpu, cache.sizeBytes);
                    }
                });
            }
        } catch (IOException | RuntimeException e) {
            log.debug("Unable to read the CPU topology under {}", sysfsCpuRoot, e);
            return Optional.empty();
        }

        if (smt.isEmpty() && llc.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new CpuTopology(smt, llc, llcBytes));
    }

    /**
     * @return every CPU this topology knows something about, ascending
     */
    public Set<Integer> cpus() {
        Set<Integer> all = new TreeSet<>(smtSiblings.keySet());
        all.addAll(lastLevelCacheSiblings.keySet());
        return Collections.unmodifiableSet(all);
    }

    /**
     * The CPUs sharing a physical core with the given one — its SMT siblings, including itself.
     *
     * @param cpu the CPU to look up
     * @return the siblings including {@code cpu}, or an empty set when the pairing is unknown
     */
    public Set<Integer> smtSiblingsOf(int cpu) {
        return Collections.unmodifiableSet(smtSiblings.getOrDefault(cpu, Collections.emptySet()));
    }

    /**
     * The CPUs sharing a last-level cache with the given one, including itself.
     *
     * @param cpu the CPU to look up
     * @return the CPUs sharing its last-level cache, or an empty set when that is unknown
     */
    public Set<Integer> lastLevelCacheSiblingsOf(int cpu) {
        return Collections.unmodifiableSet(lastLevelCacheSiblings.getOrDefault(cpu, Collections.emptySet()));
    }

    /**
     * Identifies the last-level cache a CPU is attached to, so that two CPUs can be compared without
     * comparing whole sets. The identifier is the lowest-numbered CPU on that cache, which makes it stable
     * for a given machine but meaningless across machines.
     *
     * @param cpu the CPU to look up
     * @return the identifier of its last-level cache, or empty when that is unknown
     */
    public OptionalInt lastLevelCacheDomainOf(int cpu) {
        Set<Integer> siblings = lastLevelCacheSiblings.get(cpu);
        if (siblings == null || siblings.isEmpty()) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(Collections.min(siblings));
    }

    /**
     * @param cpu the CPU to look up
     * @return the size of its last-level cache in bytes, or empty when that is unknown — telling apart the
     * dies of a part whose cache is not the same size on each of them
     */
    public OptionalLong lastLevelCacheBytesOf(int cpu) {
        Long bytes = lastLevelCacheBytes.get(cpu);
        return bytes == null ? OptionalLong.empty() : OptionalLong.of(bytes);
    }

    /**
     * @return how many distinct last-level caches this machine has; more than one means threads can be placed
     * so that they do not share one
     */
    public int lastLevelCacheDomainCount() {
        Set<Integer> domains = new TreeSet<>();
        for (int cpu : lastLevelCacheSiblings.keySet()) {
            lastLevelCacheDomainOf(cpu).ifPresent(domains::add);
        }
        return domains.size();
    }

    /**
     * Whether every one of the given CPUs is attached to the same last-level cache — the check worth making
     * before pinning the two ends of a queue.
     *
     * @param cpus the CPUs to test; fewer than two trivially share one
     * @return {@code true} when they share a last-level cache, {@code false} when they span more than one or
     * when any of them has an unknown cache, since an unproven placement is not a good one
     */
    public boolean shareLastLevelCache(Collection<Integer> cpus) {
        if (cpus == null || cpus.size() < 2) {
            return true;
        }
        OptionalInt first = OptionalInt.empty();
        for (int cpu : cpus) {
            OptionalInt domain = lastLevelCacheDomainOf(cpu);
            if (domain.isEmpty()) {
                return false;
            }
            if (first.isEmpty()) {
                first = domain;
            } else if (first.getAsInt() != domain.getAsInt()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Finds the CPUs in the given set that share a physical core with another CPU of that same set, which is
     * how two threads meant to run in parallel end up competing for one core's execution units.
     *
     * @param cpus the CPUs to test
     * @return those of them sharing a physical core with another, ascending; empty when each sits on a core
     * of its own or when the pairing is unknown
     */
    public Set<Integer> smtSiblingCollisions(Collection<Integer> cpus) {
        Set<Integer> collisions = new TreeSet<>();
        if (cpus == null) {
            return collisions;
        }
        Set<Integer> distinct = new TreeSet<>(cpus);
        for (int cpu : distinct) {
            for (int sibling : smtSiblingsOf(cpu)) {
                if (sibling != cpu && distinct.contains(sibling)) {
                    collisions.add(cpu);
                    break;
                }
            }
        }
        return collisions;
    }

    private static Optional<Cache> deepestCache(Path cacheDirectory) {
        if (!Files.isDirectory(cacheDirectory)) {
            return Optional.empty();
        }
        Cache deepest = null;
        try (DirectoryStream<Path> indexes = Files.newDirectoryStream(cacheDirectory, "index*")) {
            for (Path index : indexes) {
                Optional<String> level = readLine(index.resolve("level"));
                Optional<Set<Integer>> shared = readCpuList(index.resolve("shared_cpu_list"));
                if (level.isEmpty() || shared.isEmpty()) {
                    continue;
                }
                int depth = Integer.parseInt(level.get());
                if (deepest == null || depth > deepest.level) {
                    deepest = new Cache(depth, shared.get(), readSize(index.resolve("size")));
                }
            }
        } catch (IOException | RuntimeException e) {
            log.debug("Unable to read the cache hierarchy under {}", cacheDirectory, e);
            return Optional.empty();
        }
        return Optional.ofNullable(deepest);
    }

    /**
     * Parses the comma-and-dash CPU lists {@code sysfs} writes, of the form {@code 0-5,12-17} or {@code 6,18}.
     */
    static Optional<Set<Integer>> readCpuList(Path file) {
        return readLine(file).map(line -> {
            Set<Integer> cpus = new LinkedHashSet<>();
            for (String range : line.split(",")) {
                String trimmed = range.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                int dash = trimmed.indexOf('-');
                if (dash < 0) {
                    cpus.add(Integer.parseInt(trimmed));
                } else {
                    int from = Integer.parseInt(trimmed.substring(0, dash));
                    int to = Integer.parseInt(trimmed.substring(dash + 1));
                    for (int cpu = from; cpu <= to; cpu++) {
                        cpus.add(cpu);
                    }
                }
            }
            return cpus.isEmpty() ? null : (Set<Integer>) new TreeSet<>(cpus);
        });
    }

    /**
     * Parses the {@code 32768K} form {@code sysfs} writes cache sizes in.
     */
    private static long readSize(Path file) {
        return readLine(file).map(line -> {
            char unit = line.charAt(line.length() - 1);
            if (Character.isDigit(unit)) {
                return Long.parseLong(line);
            }
            long value = Long.parseLong(line.substring(0, line.length() - 1));
            switch (Character.toUpperCase(unit)) {
                case 'K':
                    return value * 1024L;
                case 'M':
                    return value * 1024L * 1024L;
                case 'G':
                    return value * 1024L * 1024L * 1024L;
                default:
                    return 0L;
            }
        }).orElse(0L);
    }

    private static Optional<String> readLine(Path file) {
        try {
            if (!Files.isReadable(file)) {
                return Optional.empty();
            }
            String content = new String(Files.readAllBytes(file), StandardCharsets.US_ASCII).trim();
            return content.isEmpty() ? Optional.empty() : Optional.of(content);
        } catch (IOException | RuntimeException e) {
            return Optional.empty();
        }
    }

    private static final class Cache {

        private final int level;

        private final Set<Integer> sharedCpus;

        private final long sizeBytes;

        private Cache(int level, Set<Integer> sharedCpus, long sizeBytes) {
            this.level = level;
            this.sharedCpus = sharedCpus;
            this.sizeBytes = sizeBytes;
        }
    }
}
