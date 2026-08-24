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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

class UnsafeOperationsImplAllTest {

    /**
     * The whole point of impl-all: with all three version-specific implementations on the classpath
     * (including the Java 25 one compiled to a newer class-file version), the API must resolve to the
     * implementation matching the running JDK without a class-loading failure from the others.
     */
    @Test
    void resolvesTheImplementationMatchingTheRunningJdk() {
        UnsafeOperations unsafe = UnsafeOperationsApi.get();

        int feature = Runtime.version().feature();
        String expectedSimpleName;
        if (feature >= 11 && feature <= 14) {
            expectedSimpleName = "UnsafeOperationsJava11To14Impl";
        } else if (feature >= 15 && feature <= 24) {
            expectedSimpleName = "UnsafeOperationsJava15To24Impl";
        } else if (feature >= 25) {
            expectedSimpleName = "UnsafeOperationsJava25Impl";
        } else {
            throw new AssertionError("Test is running on an unsupported JDK feature version: " + feature);
        }

        assertThat(unsafe).isNotNull();
        assertThat(unsafe.getClass().getSimpleName()).isEqualTo(expectedSimpleName);
    }

    @Test
    void selectedImplementationCanPerformAnUnsafeOperation() {
        UnsafeOperations unsafe = UnsafeOperationsApi.get();

        // A direct buffer whose cleaner we can invoke exercises the version-specific Unsafe wiring end-to-end.
        ByteBuffer direct = ByteBuffer.allocateDirect(64);
        assertThat(unsafe.arrayBaseOffset(byte[].class)).isPositive();
        unsafe.invokeCleanerIfNeeded(direct);
    }

    @Test
    void exactlyOneProviderClaimsTheCurrentJdk() {
        long matching = java.util.ServiceLoader.load(UnsafeOperationsProvider.class).stream()
                .map(java.util.ServiceLoader.Provider::get)
                .filter(UnsafeOperationsProvider::isForCurrentJDK)
                .count();
        assertThat(matching).isEqualTo(1);
    }
}
