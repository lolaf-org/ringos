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

import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * The measurement settings and thread pinning shared by the three topology benchmarks
 * ({@link SpScRingBufferBenchmark}, {@link MpScRingBufferBenchmark}, {@link MpMcRingBufferBenchmark}).
 *
 * <p>Producer and consumer threads are pinned, and the defaults deliberately keep every thread of a
 * group on one CCD so that the handoff stays inside a shared L3. Splitting a group across dies makes
 * each handoff cross the interconnect, which costs several times an intra-die transfer and dominates
 * the algorithmic differences the benchmark exists to show. Consumers take cores upwards from
 * {@value ConsumerThreadAffinityState#FIRST_CORE_PROPERTY} (6 by default) and producers upwards from
 * {@value ProducerThreadAffinityState#FIRST_CORE_PROPERTY} (9 by default), so the widest group fills
 * cores 6 to 11 exactly, one thread to a physical core. The single-threaded benchmarks are pinned too,
 * to the first producer core: there is no handoff to place, but an unpinned thread is free to migrate
 * between dies whose caches and clocks differ, which shows up as run-to-run noise.
 *
 * <p>Those defaults suit a six-core CCD holding cores 6 to 11; on other topologies set both
 * properties to the first core of the range you want, remembering that JMH forks a child JVM and
 * only forwards the properties it was itself launched with.
 *
 * <p>The annotations below are inherited by each subclass, so all three run under one set of
 * measurement settings.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 2, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsPrepend = {
        "--enable-native-access=ALL-UNNAMED",
        "-XX:-RestrictContended", "-XX:ContendedPaddingWidth=64",
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens", "java.base/jdk.internal.vm.annotation=ALL-UNNAMED",
        "-Xmx1g", "-Xms1g"})
public abstract class AbstractRingBufferBenchmark {

    /**
     * Runs one topology's benchmarks, as each subclass's {@code main} does.
     *
     * @param benchmarkClass the benchmark class to include in the run
     * @throws RunnerException if the run fails
     */
    protected static void run(Class<?> benchmarkClass) throws RunnerException {
        new Runner(new OptionsBuilder()
                .include(benchmarkClass.getSimpleName())
                .addProfiler("gc")
                .resultFormat(ResultFormatType.JSON)
                .build()).run();
    }
}
