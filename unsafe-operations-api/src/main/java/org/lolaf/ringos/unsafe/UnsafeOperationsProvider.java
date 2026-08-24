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
package org.lolaf.ringos.unsafe;

import java.util.List;

/**
 * Lightweight SPI that selects and lazily builds the {@link UnsafeOperations} implementation matching the
 * running JDK.
 *
 * <p><b>Why a separate provider instead of {@link UnsafeOperations} directly.</b> The concrete
 * {@code UnsafeOperations} implementations are compiled to different class-file versions (e.g. the Java 25
 * implementation is compiled with {@code --release 25}). A single "all JDKs" dependency therefore carries
 * classes that cannot even be <em>loaded</em> on an older JVM — attempting to do so throws
 * {@link UnsupportedClassVersionError}. Because {@link java.util.ServiceLoader} must load a provider class
 * before it can call any method on it, the JDK-version test cannot live on {@code UnsafeOperations} itself.
 *
 * <p>Each version-specific implementation module ships its own provider next to its implementation. Provider
 * implementations must:
 * <ul>
 *   <li>be compiled to the lowest supported class-file version so they load on <em>every</em> supported JDK
 *       (in a module whose implementation targets a newer bytecode version, the provider must be compiled
 *       separately at that lowest release);</li>
 *   <li>name the version-specific {@code UnsafeOperations} implementation only as a {@link String} via
 *       {@link #implementationClassName()} — never as a typed reference — so that (possibly newer-bytecode)
 *       class is loaded by {@link UnsafeOperationsApi} exclusively after {@link #isForCurrentJDK()} has
 *       confirmed the running JDK can load it.</li>
 * </ul>
 */
public interface UnsafeOperationsProvider {

    /**
     * @return {@code true} if the {@link UnsafeOperations} implementation this provider names targets the JDK
     * the application is currently running on. Implementations must decide this from {@link Runtime#version()}
     * alone
     */
    boolean isForCurrentJDK();

    /**
     * @return the fully-qualified name of the {@link UnsafeOperations} implementation to instantiate. It is
     * returned as a string (rather than a {@link Class}) so this provider never links the implementation
     * class; {@link UnsafeOperationsApi} loads it reflectively, and only when {@link #isForCurrentJDK()} is
     * {@code true}.
     */
    String implementationClassName();

    /**
     * @return the {@code java.base} packages this provider's implementation needs opened to it — i.e. the JVM
     * must have been started with {@code --add-opens java.base/<pkg>=ALL-UNNAMED} for each. Different
     * implementations need different packages (e.g. the Java 11-14 one also needs {@code jdk.internal.ref} and
     * {@code sun.nio.ch} for cleaner access), so the list is provider-specific. Package names only (no
     * {@code java.base/} prefix or {@code =ALL-UNNAMED} suffix).
     */
    List<String> requiredOpenPackages();
}
