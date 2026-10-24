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

import org.lolaf.ringos.rb.spi.AbstractBroadcastByteRingBuffer;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A single-producer broadcast byte ring buffer over {@link VarHandle}; see {@link AbstractBroadcastByteRingBuffer}
 * for the protocol. Header fields are in native order, never seen outside the buffer.
 */
public class MethodHandleSpBroadcastByteRingBuffer extends AbstractBroadcastByteRingBuffer {

    private static final VarHandle TAIL;
    private static final VarHandle TAIL_INTENT;
    private static final VarHandle INT = MethodHandles.byteBufferViewVarHandle(int[].class, ByteOrder.nativeOrder());
    private static final VarHandle LONG = MethodHandles.byteBufferViewVarHandle(long[].class, ByteOrder.nativeOrder());

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            TAIL = lookup.findVarHandle(MethodHandleSpBroadcastByteRingBuffer.class, "tail", long.class);
            TAIL_INTENT = lookup.findVarHandle(MethodHandleSpBroadcastByteRingBuffer.class, "tailIntent", long.class);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    public MethodHandleSpBroadcastByteRingBuffer(int capacity) {
        super(newBuffer(capacity), capacity);
    }

    private static ByteBuffer newBuffer(int capacity) {
        ByteBuffer aligned = ByteBuffer.allocateDirect(capacity + 64).alignedSlice(64);
        aligned.limit(capacity);
        return aligned.slice();
    }

    @Override
    protected long getTailAcquire() {
        return (long) TAIL.getAcquire(this);
    }

    @Override
    protected void setTailRelease(long value) {
        TAIL.setRelease(this, value);
    }

    @Override
    protected long getTailIntentAcquire() {
        return (long) TAIL_INTENT.getAcquire(this);
    }

    @Override
    protected void setTailIntentRelease(long value) {
        TAIL_INTENT.setRelease(this, value);
    }

    @Override
    protected int getIntPlain(int index) {
        return (int) INT.get(buffer, index);
    }

    @Override
    protected void setIntPlain(int index, int value) {
        INT.set(buffer, index, value);
    }

    @Override
    protected long getLongPlain(int index) {
        return (long) LONG.get(buffer, index);
    }

    @Override
    protected void setLongPlain(int index, long value) {
        LONG.set(buffer, index, value);
    }
}
