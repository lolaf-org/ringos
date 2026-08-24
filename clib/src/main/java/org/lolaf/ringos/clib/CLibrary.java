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

import java.time.Duration;

/**
 * The handful of Linux system calls ringos needs in order to make a thread behave predictably: the timer slack
 * that decides how late a short park may return, and the scheduling policy that decides whether the thread is
 * preempted at all.
 *
 * <p>Obtained from {@link CLibraryApi#get()}, never constructed directly. The default methods here <b>are</b> the
 * implementation used off Linux, or on Linux when the C library could not be loaded: each one does nothing and
 * reports the neutral value, so a caller never has to ask whether the platform supports any of this. The
 * Linux-backed implementation overrides them all.
 *
 * <p>That fallback is why {@link #getTimerSlack()} returning {@link Duration#ZERO} is the signal to leave timer
 * slack alone — a real thread never has a slack of zero nanoseconds. The two setters are less forthcoming: they
 * return {@code 0} both when a real call succeeded and when the no-op implementation ignored them, so treat their
 * result as "nothing went wrong", not as proof the setting took.
 *
 * @see <a href="https://man7.org/linux/man-pages/man2/prctl.2.html">prctl(2)</a>
 * @see <a href="https://man7.org/linux/man-pages/man2/sched_setscheduler.2.html">sched_setscheduler(2)</a>
 */
public interface CLibrary {

    /**
     * The {@code errno} a call reports when it was refused for want of privilege — asking for a real-time
     * policy without {@code CAP_SYS_NICE}, most often. The one failure worth telling a user how to fix.
     */
    int EPERM = 1;

    /**
     * The {@code errno} a call reports when the request itself was malformed: a priority outside the range the
     * policy allows, or a policy this system call cannot configure.
     */
    int EINVAL = 22;

    /**
     * Reads the timer slack of the <b>calling thread</b> — how far past its requested expiry the kernel is
     * allowed to let a timer or a short {@code park} run, which it rounds sleeps up by. Usually 50 microseconds
     * unless it has been narrowed.
     *
     * @return the calling thread's timer slack, or {@link Duration#ZERO} when this platform cannot report it —
     * a real thread never has zero slack, so zero means "unknown", not "perfectly precise"
     */
    default Duration getTimerSlack() {
        return Duration.ofNanos(0L);
    }

    /**
     * Narrows (or widens) the timer slack of the <b>calling thread</b>, so that its short parks are honoured
     * rather than rounded up to the default 50 microseconds. It applies to that one thread and outlives any
     * particular use of it — see {@code TimerSlackAwareBackoffIdleStrategy}, which sets it on the thread that
     * will idle.
     *
     * @param slack the new slack; the kernel takes it in nanoseconds, and a value of zero restores the
     *              inherited default rather than removing slack altogether
     * @return {@code 0} on success, otherwise the {@code errno} the underlying {@code prctl} failed with — see
     * {@link #EPERM} and {@link #EINVAL}. A no-op implementation also returns {@code 0}, so this does not prove
     * the slack changed — read it back with {@link #getTimerSlack()} if it matters
     */
    default int setTimerSlack(Duration slack) {
        return 0;
    }

    /**
     * Moves the <b>calling thread</b> onto another scheduling policy — {@link LinuxScheduler#SCHED_FIFO} above
     * all, which takes a latency-critical thread off the fair scheduler so it is not preempted by ordinary work.
     *
     * <p>Scheduling policy on Linux is a per-thread property, so this retunes the one thread that calls it and
     * leaves every other thread in the JVM on the policy it had. Call it from the thread you mean to privilege —
     * the polling loop itself — the way {@code TimerSlackAwareBackoffIdleStrategy} sets timer slack on the thread
     * that will idle.
     *
     * <p>"The calling thread" covers the threads that already exist, not the ones that do not yet: the kernel
     * gives a new thread the policy of the thread that created it. Retune a thread that goes on to spawn others
     * and they start out retuned as well, which is worth knowing before doing this on a thread that owns a pool.
     *
     * <p>A real-time policy needs {@code CAP_SYS_NICE} (or root). Without it the call simply fails, and a
     * real-time thread that spins without ever blocking can lock up a core against everything else on the
     * machine — so raise the policy deliberately, on a thread you know yields.
     *
     * @param priority  the static priority for the policy, 1-99 for the real-time policies and 0 for the
     *                  others; higher preempts lower
     * @param scheduler the policy to switch to
     * @return {@code 0} on success, otherwise the {@code errno} the underlying {@code sched_setscheduler}
     * failed with — {@link #EPERM} for want of privilege, {@link #EINVAL} for a priority the policy does not
     * allow. A no-op implementation also returns {@code 0}
     */
    default int setThreadScheduler(int priority, LinuxScheduler scheduler) {
        return 0;
    }
}
