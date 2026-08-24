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

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The Linux scheduling policies that can be passed to
 * {@link CLibrary#setThreadScheduler(int, LinuxScheduler)}, each carrying the numeric policy constant the
 * {@code sched_setscheduler} system call expects.
 *
 * <p>The split that matters is between the real-time policies ({@link #SCHED_FIFO}, {@link #SCHED_RR}), which
 * take a thread out of the fair scheduler's hands and require {@code CAP_SYS_NICE}, and the rest, which are
 * variations on how the fair scheduler treats an ordinary thread.
 *
 * <p>Linux's two remaining policies are deliberately absent. {@code SCHED_DEADLINE} cannot be set through
 * {@code sched_setscheduler} at all — it needs the runtime, period and deadline that only {@code sched_setattr}
 * carries, and the call rejects it with {@code EINVAL} — so offering it here would only hand callers a constant
 * that always fails. {@code SCHED_ISO} was never implemented in mainline Linux.
 *
 * @see <a href="https://man7.org/linux/man-pages/man7/sched.7.html">sched(7)</a>
 */
@AllArgsConstructor
@Getter
public
enum LinuxScheduler {
    /**
     * The default policy, the completely fair scheduler: every thread gets a share of the CPU and is preempted
     * to give the others theirs. Static priority is always 0 here; niceness, not priority, biases the share.
     */
    SCHED_OTHER(0), // CFS
    /**
     * Real-time, run-to-completion: the thread runs until it blocks, yields, or a higher-priority real-time
     * thread becomes runnable — it is never preempted merely because its turn is up. The policy for a
     * latency-critical polling thread, and the one that will monopolise a core if that thread never yields.
     */
    SCHED_FIFO(1),// Real-time FIFO
    /**
     * Real-time round-robin: {@link #SCHED_FIFO} plus a time quantum, so threads of equal priority take turns
     * instead of the first one holding the CPU until it blocks.
     */
    SCHED_RR(2), // Real-time round-robin
    /**
     * Fair scheduling for throughput work: the kernel treats the thread as CPU-bound and avoids waking it
     * eagerly, which suits batch jobs and hurts anything latency-sensitive.
     */
    SCHED_BATCH(3), // Batch
    /**
     * The weakest policy: the thread runs only when nothing else wants the CPU. For background work that should
     * never take a cycle from the message path.
     */
    SCHED_IDLE(5);// Idle

    /**
     * The numeric policy constant passed to {@code sched_setscheduler}, as defined by the kernel's
     * {@code <sched.h>}.
     */
    private final int code;
}
