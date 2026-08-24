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
package org.lolaf.ringos.clib;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Exercises the Linux-backed implementation directly, so it only runs where those system calls exist.
 * <p>
 * Every assertion is about what the library does — that a value written comes back, that a failure reports why —
 * never about what this particular machine happens to be configured to, which is not the library's doing. The
 * calls it makes are per-thread and outlive the test, so each one is undone afterwards: JUnit hands the same
 * thread to whatever runs next.
 */
@EnabledOnOs(OS.LINUX)
class CLibraryImplTest {

    /**
     * Field 41 of {@code /proc/[tid]/stat} is the thread's scheduling policy, and the only way to see it from
     * Java — {@code sched_getscheduler} is not bound.
     */
    private static final int POLICY_FIELD_INDEX_AFTER_COMM = 38;

    private CLibraryImpl cLibrary;
    private Duration originalSlack;

    private static int currentThreadPolicy() throws Exception {
        String stat = new String(Files.readAllBytes(Paths.get("/proc/thread-self/stat")));
        String[] afterComm = stat.substring(stat.lastIndexOf(')') + 2).trim().split("\\s+");
        return Integer.parseInt(afterComm[POLICY_FIELD_INDEX_AFTER_COMM]);
    }

    @BeforeEach
    void setUp() {
        cLibrary = new CLibraryImpl();
        originalSlack = cLibrary.getTimerSlack();
    }

    @AfterEach
    void restoreThreadState() throws Exception {
        cLibrary.setTimerSlack(originalSlack);
        if (currentThreadPolicy() != LinuxScheduler.SCHED_OTHER.getCode()) {
            cLibrary.setThreadScheduler(0, LinuxScheduler.SCHED_OTHER);
        }
    }

    @Test
    void readsTheCallingThreadsTimerSlack() {
        // a real thread always has some slack; the default is 50 micros, but a tuned machine is not a failure
        assertThat(originalSlack).isGreaterThan(Duration.ZERO);
    }

    @Test
    void narrowsTheCallingThreadsTimerSlack() {
        assertThat(cLibrary.setTimerSlack(Duration.ofNanos(5000))).isZero();
        assertThat(cLibrary.getTimerSlack()).isEqualTo(Duration.ofNanos(5000));

        // below the 50 micros default the kernel honours the request, it just stops buying anything
        assertThat(cLibrary.setTimerSlack(Duration.ofNanos(1000))).isZero();
        assertThat(cLibrary.getTimerSlack()).isEqualTo(Duration.ofNanos(1000));
    }

    @Test
    void movesTheCallingThreadOntoAnotherPolicy() throws Exception {
        int returnCode = cLibrary.setThreadScheduler(0, LinuxScheduler.SCHED_BATCH);
        assumeTrue(returnCode == 0, "kernel refused SCHED_BATCH with errno " + returnCode);

        assertThat(currentThreadPolicy()).isEqualTo(LinuxScheduler.SCHED_BATCH.getCode());
    }

    @Test
    void leavesAThreadThatWasAlreadyRunningOnItsOwnPolicy() throws Exception {
        CountDownLatch observerStarted = new CountDownLatch(1);
        CountDownLatch policyChanged = new CountDownLatch(1);
        int[] policyOfOtherThread = new int[1];
        Thread other = new Thread(() -> {
            try {
                observerStarted.countDown();
                policyChanged.await();
                policyOfOtherThread[0] = currentThreadPolicy();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }, "policy-observer");
        other.start();
        observerStarted.await();

        int returnCode = cLibrary.setThreadScheduler(0, LinuxScheduler.SCHED_BATCH);
        assumeTrue(returnCode == 0, "kernel refused SCHED_BATCH with errno " + returnCode);
        policyChanged.countDown();
        other.join();

        assertThat(policyOfOtherThread[0]).isEqualTo(LinuxScheduler.SCHED_OTHER.getCode());
    }

    @Test
    void isInheritedByAThreadStartedAfterwards() throws Exception {
        int returnCode = cLibrary.setThreadScheduler(0, LinuxScheduler.SCHED_BATCH);
        assumeTrue(returnCode == 0, "kernel refused SCHED_BATCH with errno " + returnCode);

        // the kernel hands a new thread its creator's policy, so "only the caller" holds for threads that
        // already exist — anything spawned from a retuned thread starts out retuned too
        int[] policyOfChildThread = new int[1];
        Thread child = new Thread(() -> {
            try {
                policyOfChildThread[0] = currentThreadPolicy();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }, "policy-heir");
        child.start();
        child.join();

        assertThat(policyOfChildThread[0]).isEqualTo(LinuxScheduler.SCHED_BATCH.getCode());
    }

    @Test
    void reportsWhyARealTimePolicyWasRefused() {
        int returnCode = cLibrary.setThreadScheduler(99, LinuxScheduler.SCHED_FIFO);

        // whether this machine grants CAP_SYS_NICE is not the library's doing; reporting the reason is
        assertThat(returnCode).isIn(0, CLibrary.EPERM);
    }

    @Test
    void reportsAPriorityThePolicyDoesNotAllow() {
        // SCHED_OTHER accepts a static priority of 0 and nothing else
        assertThat(cLibrary.setThreadScheduler(50, LinuxScheduler.SCHED_OTHER)).isEqualTo(CLibrary.EINVAL);
    }
}
