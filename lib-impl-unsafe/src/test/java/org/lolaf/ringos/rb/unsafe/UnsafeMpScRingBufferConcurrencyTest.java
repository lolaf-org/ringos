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
package org.lolaf.ringos.rb.unsafe;

import org.lolaf.ringos.rb.RingBufferBuilder;

import org.lolaf.ringos.rb.testkit.AbstractMpScRingBufferConcurrencyTest;

/**
 * Runs the shared ring-buffer test kit against the {@code Unsafe} implementation for Java 15 and later.
 */
class UnsafeMpScRingBufferConcurrencyTest extends AbstractMpScRingBufferConcurrencyTest {

    @Override
    protected RingBufferBuilder builder() {
        return new UnsafeRbBuilder();
    }
}
