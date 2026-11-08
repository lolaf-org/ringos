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
package org.lolaf.ringos.rb;

import java.nio.ByteBuffer;

/**
 * Writes a frame's payload in place, into the range a byte ring buffer claimed for it.
 *
 * <p>Make it a static field so it captures nothing: passing state through {@code arg} is what keeps a write free of
 * allocation.
 *
 * @param <A> type of the argument
 */
@FunctionalInterface
public interface ByteTranslator<A> {

    /**
     * Writes exactly {@code length} bytes at {@code index}, with absolute puts. {@code buffer} is the ring's own: its
     * position and limit must not be changed, nor anything written outside the range, which belongs to other frames.
     * A broadcast ring may call this under its writers' spin lock, which is not reentrant: creating or closing a gating
     * reader here, or seeking one back, hangs the thread.
     */
    void translate(ByteBuffer buffer, int index, int length, A arg);
}
