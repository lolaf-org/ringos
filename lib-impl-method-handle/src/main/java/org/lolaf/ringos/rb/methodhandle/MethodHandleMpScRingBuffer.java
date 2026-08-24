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

import org.lolaf.ringos.rb.spi.AbstractSlotFlaggedMpScRingBuffer;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * A slot-flagged multi-producer, single-consumer ring buffer over {@link VarHandle}.
 *
 * <p>See {@link AbstractSlotFlaggedMpScRingBuffer} for the protocol and for why this one carries no sequence
 * array. {@link MethodHandlePooledMpScRingBuffer} is the implementation to reach for when the slots must hold
 * pooled element instances.
 */
public class MethodHandleMpScRingBuffer<T> extends AbstractSlotFlaggedMpScRingBuffer<T> {

    private static final VarHandle HEAD;
    private static final VarHandle TAIL;
    private static final VarHandle BUFFER_ARRAY;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            HEAD = lookup.findVarHandle(MethodHandleMpScRingBuffer.class, "head", long.class);
            TAIL = lookup.findVarHandle(MethodHandleMpScRingBuffer.class, "tail", long.class);
            BUFFER_ARRAY = MethodHandles.arrayElementVarHandle(Object[].class);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    public MethodHandleMpScRingBuffer(int capacity, boolean bufferPaddingEnabled) {
        super(capacity, bufferPaddingEnabled);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected T getBufferElementAcquire(int index) {
        return (T) BUFFER_ARRAY.getVolatile(buffer, index);
    }

    @Override
    protected void setBufferElementRelease(int index, T value) {
        BUFFER_ARRAY.setRelease(buffer, index, value);
    }

    @Override
    protected void clearBufferElementPlain(int index) {
        BUFFER_ARRAY.set(buffer, index, null);
    }

    @Override
    protected boolean tailCompareAndSwap(long current, long next) {
        return TAIL.compareAndSet(this, current, next);
    }

    @Override
    protected void setHeadRelease(long value) {
        HEAD.setRelease(this, value);
    }

    @Override
    protected void resetTailAndHead() {
        TAIL.setVolatile(this, 0L);
        HEAD.setVolatile(this, 0L);
    }

    /**
     * Read with acquire semantics rather than plainly: a producer calls this to decide a slot has been freed,
     * and must see the consumer's emptying of that slot along with the advanced head that announces it.
     */
    @Override
    public long getCurrentHead() {
        return (long) HEAD.getVolatile(this);
    }

    @Override
    public long getCurrentTail() {
        return (long) TAIL.getVolatile(this);
    }
}
