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

import com.sun.jna.Platform;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Entry point to the {@link CLibrary} calls, resolved once for the lifetime of the JVM.
 *
 * <p>Everything here is best-effort by design: on Linux it loads the C library through JNA, and on any other
 * platform — or when that load fails, for want of a native library to bind to or of anywhere to unpack JNA's
 * own — it logs and falls back to {@link CLibrary}'s do-nothing default methods. Resolution therefore never
 * throws, neither an exception nor a {@link LinkageError}, and callers can tune timer slack or scheduling
 * policy unconditionally, getting a silent no-op where the platform cannot oblige.
 */
@Slf4j
@UtilityClass
public class CLibraryApi {

    private static final CLibrary IMPL = getImpl();

    /**
     * @return the resolved implementation, the Linux-backed one where it could be loaded and the no-op
     * otherwise. Never {@code null}, and always the same instance — it is stateless and safe to share
     */
    public static CLibrary get() {
        return IMPL;
    }

    private static CLibrary getImpl() {
        if (Platform.isLinux()) {
            try {
                return new CLibraryImpl();
            } catch (Exception | LinkageError e) {
                // LinkageError, not just Exception: Native.load throws UnsatisfiedLinkError, which is how this
                // fails in practice — a container whose tmpdir is mounted noexec, so JNA cannot unpack its own
                // native library. Catching Exception alone let that escape a static initialiser, killing the
                // first caller instead of degrading to the no-op implementation.
                log.warn("Unable to load C library, will default to a default void implementation", e);
            }
        }
        return new CLibrary() {
        };
    }
}
