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
package org.lolaf.ringos.rb.unsafe11;

import org.lolaf.ringos.rb.RingBufferBuilderProvider;

/**
 * Selects {@link J11UnsafeRbBuilder} on Java 11 when {@code jdk.internal.misc} is opened. Names the
 * builder by string so this provider never links the Unsafe-based classes; they are loaded only once selected.
 */
public final class J11UnsafeRingBufferBuilderProvider implements RingBufferBuilderProvider {

    @Override
    public boolean isForCurrentRuntime() {
        return isJdkInternalMiscOpen() && Runtime.version().feature() == 11;
    }

    @Override
    public String implementationClassName() {
        return "org.lolaf.ringos.rb.unsafe11.J11UnsafeRbBuilder";
    }
}
