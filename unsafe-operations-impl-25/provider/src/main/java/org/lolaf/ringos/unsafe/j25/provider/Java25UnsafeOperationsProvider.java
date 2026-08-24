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
package org.lolaf.ringos.unsafe.j25.provider;

import org.lolaf.ringos.unsafe.UnsafeOperationsProvider;

import org.lolaf.ringos.unsafe.UnsafeOperationsApi;

import java.util.List;

/**
 * Selects UnsafeOperationsJava25Impl on Java 25+.
 *
 * <p>Its implementation ({@code unsafe-operations-impl-25-impl}) is compiled to a newer class-file version
 * than this provider. This provider lives in a separate module compiled to the lowest supported release, so it
 * loads on any JDK; it names the implementation only by string and depends on it at runtime, so the
 * implementation is loaded reflectively by {@link UnsafeOperationsApi} only on Java 25+.
 */
public final class Java25UnsafeOperationsProvider implements UnsafeOperationsProvider {

    @Override
    public boolean isForCurrentJDK() {
        return Runtime.version().feature() >= 25;
    }

    @Override
    public String implementationClassName() {
        return "org.lolaf.ringos.unsafe.j25.UnsafeOperationsJava25Impl";
    }

    @Override
    public List<String> requiredOpenPackages() {
        return List.of("jdk.internal.misc");
    }
}
