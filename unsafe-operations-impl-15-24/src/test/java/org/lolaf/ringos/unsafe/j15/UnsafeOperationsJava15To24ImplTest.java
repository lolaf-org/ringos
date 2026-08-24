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
package org.lolaf.ringos.unsafe.j15;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

class UnsafeOperationsJava15To24ImplTest {

    @Test
    void testInvokeCleanerIfNeededDoesNotCrashIfCleanedTwice() {
        try {
            UnsafeOperationsJava15To24Impl unsafe = new UnsafeOperationsJava15To24Impl();

            ByteBuffer bb = ByteBuffer.allocateDirect(128);
            unsafe.invokeCleanerIfNeeded(bb);
            unsafe.invokeCleanerIfNeeded(bb);
        } catch (Throwable ex) {
            Assertions.fail("should not have failed", ex);
        }
    }
}
