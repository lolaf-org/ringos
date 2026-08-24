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

import com.sun.jna.Native;


import java.time.Duration;

/**
 * The Linux-backed {@link CLibrary}, binding the calls to the system C library through JNA.
 *
 * <p>Package-private and instantiated only by {@link CLibraryApi}, which catches the failure to load and falls
 * back to the no-op defaults — so nothing outside this package has to handle a platform without these calls.
 */
class CLibraryImpl implements CLibrary {

    private final CLibraryJna clib;

    /**
     * Binds to the system C library.
     *
     * @throws UnsatisfiedLinkError if it cannot be loaded, which {@link CLibraryApi} treats as "this platform
     *                              does not offer these calls"
     */
    public CLibraryImpl() {
        clib = Native.load("c", CLibraryJna.class);
    }

    @Override
    public Duration getTimerSlack() {
        int slackNanos = clib.prctl(CLibraryJna.PR_GET_TIMERSLACK, 0, 0, 0, 0);
        // a failed read would otherwise become a negative Duration; zero is what the interface documents as
        // "could not be read", and is a value no real thread has
        return slackNanos == CLibraryJna.FAILED ? Duration.ZERO : Duration.ofNanos(slackNanos);
    }

    @Override
    public int setTimerSlack(Duration slack) {
        return errnoOf(clib.prctl(CLibraryJna.PR_SET_TIMERSLACK, slack.toNanos(), 0, 0, 0));
    }

    @Override
    public int setThreadScheduler(int priority, LinuxScheduler scheduler) {
        CLibraryJna.SchedParam param = new CLibraryJna.SchedParam();
        param.sched_priority = priority;
        return errnoOf(clib.sched_setscheduler(CLibraryJna.CALLING_THREAD, scheduler.getCode(), param));
    }

    /**
     * Turns a C return code into the result the {@link CLibrary} methods promise: the reason for a failure
     * rather than the bare fact of one. JNA captures {@code errno} per call on the calling thread, so it is
     * still the failing call's when read here.
     */
    private int errnoOf(int returnCode) {
        return returnCode == CLibraryJna.FAILED ? Native.getLastError() : 0;
    }
}
