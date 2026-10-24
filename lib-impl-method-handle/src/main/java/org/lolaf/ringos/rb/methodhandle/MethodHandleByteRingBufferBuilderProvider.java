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
package org.lolaf.ringos.rb.methodhandle;

import org.lolaf.ringos.rb.ByteRingBufferBuilderProvider;

/**
 * Selects {@link MethodHandleByteRbBuilder}, which runs on every supported runtime with no JVM flags. At
 * {@link #LAST_RESORT}, like {@link MethodHandleRingBufferBuilderProvider}, so a faster builder would win.
 */
public final class MethodHandleByteRingBufferBuilderProvider implements ByteRingBufferBuilderProvider {

    @Override
    public boolean isForCurrentRuntime() {
        return true;
    }

    @Override
    public int priority() {
        return LAST_RESORT;
    }

    @Override
    public String implementationClassName() {
        return "org.lolaf.ringos.rb.methodhandle.MethodHandleByteRbBuilder";
    }
}
