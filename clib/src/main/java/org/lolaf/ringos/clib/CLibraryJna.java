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

import com.sun.jna.Library;
import com.sun.jna.Structure;

/**
 * The raw JNA binding to the system C library: one method per system call, with C's signatures and C's return
 * conventions. {@link CLibraryImpl} is the layer that turns these into the {@link CLibrary} API.
 */
interface CLibraryJna extends Library {

    /**
     * What every call here returns to signal failure, leaving the reason in {@code errno}.
     */
    int FAILED = -1;

    /**
     * The thread id that {@link #sched_setscheduler} reads as "the thread making this call", saving the caller
     * from having to look its own up.
     */
    int CALLING_THREAD = 0;

    /**
     * {@code prctl} option to set the calling thread's timer slack, taking the new value in nanoseconds as
     * {@code arg2}.
     */
    int PR_SET_TIMERSLACK = 29;
    /**
     * {@code prctl} option to read the calling thread's timer slack, which comes back as the return value rather
     * than through an out parameter.
     */
    int PR_GET_TIMERSLACK = 30;

    /**
     * Operates on the calling thread. The meaning of the arguments and of the result depends entirely on
     * {@code option}: the unused trailing arguments must be passed as zero.
     *
     * @param option one of the {@code PR_} constants
     * @param arg2   the option's argument, or {@code 0}
     * @param arg3   unused by the options here; pass {@code 0}
     * @param arg4   unused by the options here; pass {@code 0}
     * @param arg5   unused by the options here; pass {@code 0}
     * @return for a get option, the value read; for a set option, {@code 0} on success. {@link #FAILED} signals
     * failure with the reason in {@code errno}
     */
    int prctl(int option, long arg2, long arg3, long arg4, long arg5);

    /**
     * C's {@code struct sched_param}, whose one field the non-deadline policies use. JNA maps it field by field,
     * which is what {@link Structure.FieldOrder} pins down.
     */
    @Structure.FieldOrder({"sched_priority"})
    class SchedParam extends Structure {
        /**
         * The static priority: 1-99 for the real-time policies, and 0 for every other policy, which rejects
         * anything else.
         */
        public int sched_priority;
    }

    /**
     * Sets the scheduling policy and priority of one thread.
     *
     * @param pid    the thread id to retune — not a process id, despite the name — or {@code 0} for the calling
     *               thread
     * @param policy the policy constant, i.e. a {@link LinuxScheduler#getCode()}
     * @param param  the priority to apply under that policy
     * @return {@code 0} on success, {@link #FAILED} on failure with the reason in {@code errno} —
     * {@link CLibrary#EPERM} when the caller lacks {@code CAP_SYS_NICE} for a real-time policy
     */
    int sched_setscheduler(int pid, int policy, SchedParam param);

}