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

import org.lolaf.ringos.rb.RingBufferBuilderProvider;

/**
 * Selects {@link MethodHandleRbBuilder}, which uses {@link java.lang.invoke.VarHandle}s only and therefore runs
 * on every supported runtime with no JVM flags at all. It claims all of them and sits at
 * {@link #LAST_RESORT}, so an {@code Unsafe} builder wins wherever one applies and this one is what remains
 * when none does — typically a JVM started without
 * {@code --add-opens java.base/jdk.internal.misc=ALL-UNNAMED}.
 */
public final class MethodHandleRingBufferBuilderProvider implements RingBufferBuilderProvider {

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
        return "org.lolaf.ringos.rb.methodhandle.MethodHandleRbBuilder";
    }
}
