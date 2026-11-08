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
package org.lolaf.ringos.rb.methodhandle16.provider;

import org.lolaf.ringos.rb.ByteRingBufferBuilderProvider;

/**
 * Selects the JDK 16+ byte ring buffers from Java 16 on, ahead of the Java 11 ones, whose copies cannot be bulk.
 */
public final class MethodHandle16ByteRingBufferBuilderProvider implements ByteRingBufferBuilderProvider {

    @Override
    public boolean isForCurrentRuntime() {
        return Runtime.version().feature() >= 16;
    }

    @Override
    public String implementationClassName() {
        return "org.lolaf.ringos.rb.methodhandle16.MethodHandle16ByteRbBuilder";
    }
}
