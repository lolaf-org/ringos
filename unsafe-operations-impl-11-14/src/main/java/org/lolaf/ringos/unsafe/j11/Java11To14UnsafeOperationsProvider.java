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
package org.lolaf.ringos.unsafe.j11;

import org.lolaf.ringos.unsafe.UnsafeOperationsProvider;

import java.util.List;

/**
 * Selects {@link UnsafeOperationsJava11To14Impl} on Java 11 to 14. Names the implementation by string (never
 * a typed reference) so this provider stays loadable on any JDK and never links the implementation itself.
 */
public final class Java11To14UnsafeOperationsProvider implements UnsafeOperationsProvider {

    @Override
    public boolean isForCurrentJDK() {
        int feature = Runtime.version().feature();
        return feature >= 11 && feature <= 14;
    }

    @Override
    public String implementationClassName() {
        return "org.lolaf.ringos.unsafe.j11.UnsafeOperationsJava11To14Impl";
    }

    @Override
    public List<String> requiredOpenPackages() {
        // jdk.internal.misc for Unsafe; jdk.internal.ref + sun.nio.ch for direct-buffer cleaner access.
        return List.of("jdk.internal.misc", "jdk.internal.ref", "sun.nio.ch");
    }
}