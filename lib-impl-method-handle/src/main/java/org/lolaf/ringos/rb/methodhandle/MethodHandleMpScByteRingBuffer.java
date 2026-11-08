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

import org.lolaf.ringos.rb.spi.AbstractMpScByteRingBuffer;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * A multi-producer, single-consumer byte ring buffer over {@link VarHandle}; see {@link AbstractMpScByteRingBuffer}
 * for the protocol. Header ints are in native order, never seen outside the buffer.
 */
public class MethodHandleMpScByteRingBuffer extends AbstractMpScByteRingBuffer {

    private static final VarHandle HEAD;
    private static final VarHandle TAIL;
    private static final VarHandle INT = MethodHandles.byteBufferViewVarHandle(int[].class, ByteOrder.nativeOrder());

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            HEAD = lookup.findVarHandle(MethodHandleMpScByteRingBuffer.class, "head", long.class);
            TAIL = lookup.findVarHandle(MethodHandleMpScByteRingBuffer.class, "tail", long.class);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    public MethodHandleMpScByteRingBuffer(int capacity) {
        super(newBuffer(capacity), capacity);
    }

    @Override
    protected long getTailVolatile() {
        return (long) TAIL.getVolatile(this);
    }

    @Override
    protected boolean tailCompareAndSwap(long current, long next) {
        return TAIL.compareAndSet(this, current, next);
    }

    @Override
    protected long getHeadAcquire() {
        return (long) HEAD.getAcquire(this);
    }

    @Override
    protected void setHeadRelease(long value) {
        HEAD.setRelease(this, value);
    }

    @Override
    protected int getLengthAcquire(int index) {
        return (int) INT.getAcquire(buffer, index);
    }

    @Override
    protected void setLengthRelease(int index, int length) {
        INT.setRelease(buffer, index, length);
    }

    @Override
    protected int getTypePlain(int index) {
        return (int) INT.get(buffer, index + TYPE_OFFSET);
    }

    @Override
    protected void setTypePlain(int index, int type) {
        INT.set(buffer, index + TYPE_OFFSET, type);
    }
}
