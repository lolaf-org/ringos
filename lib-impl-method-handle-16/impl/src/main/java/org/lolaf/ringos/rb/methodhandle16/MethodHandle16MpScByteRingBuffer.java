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
package org.lolaf.ringos.rb.methodhandle16;

import org.lolaf.ringos.rb.methodhandle.MethodHandleMpScByteRingBuffer;

import java.nio.ByteBuffer;

/**
 * {@link MethodHandleMpScByteRingBuffer} copying and zeroing with Java 16's absolute bulk transfers, which the JIT
 * turns into a memory copy.
 */
public class MethodHandle16MpScByteRingBuffer extends MethodHandleMpScByteRingBuffer {

    /**
     * As long as the longest frame, an eighth of the capacity: copied over a consumed frame to zero it.
     */
    private final ByteBuffer zeros;

    public MethodHandle16MpScByteRingBuffer(int capacity) {
        super(capacity);
        this.zeros = ByteBuffer.allocateDirect(capacity / 8);
    }

    @Override
    protected void copyIn(ByteBuffer src, int srcIndex, int index, int length) {
        buffer.put(index, src, srcIndex, length);
    }

    @Override
    protected void zero(int index, int frame) {
        buffer.put(index, zeros, 0, frame);
    }
}
