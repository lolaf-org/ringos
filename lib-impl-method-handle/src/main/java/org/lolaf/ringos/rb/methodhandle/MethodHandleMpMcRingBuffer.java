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

import org.lolaf.ringos.rb.spi.AbstractMpMcRingBuffer;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.function.IntFunction;

public class MethodHandleMpMcRingBuffer<T> extends AbstractMpMcRingBuffer<T> {

    private static final VarHandle HEAD;
    private static final VarHandle TAIL;
    private static final VarHandle SEQUENCE_ARRAY;
    private static final VarHandle BUFFER_ARRAY;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            HEAD = lookup.findVarHandle(MethodHandleMpMcRingBuffer.class, "head", long.class);
            TAIL = lookup.findVarHandle(MethodHandleMpMcRingBuffer.class, "tail", long.class);
            SEQUENCE_ARRAY = MethodHandles.arrayElementVarHandle(long[].class);
            BUFFER_ARRAY = MethodHandles.arrayElementVarHandle(Object[].class);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    public MethodHandleMpMcRingBuffer(int capacity, boolean bufferPaddingEnabled, IntFunction<T> elementInstanceProducer) {
        super(capacity, bufferPaddingEnabled, elementInstanceProducer);
    }

    @Override
    protected long getSequence(int index) {
        return (long) SEQUENCE_ARRAY.getVolatile(sequences, index);
    }

    @Override
    protected void setSequence(int index, long value) {
        SEQUENCE_ARRAY.setRelease(sequences, index, value);
    }

    @Override
    protected boolean headCompareAndSwap(long current, long next) {
        return HEAD.compareAndSet(this, current, next);
    }

    @Override
    protected boolean tailCompareAndSwap(long current, long next) {
        return TAIL.compareAndSet(this, current, next);
    }

    @Override
    protected void resetTailAndHead() {
        TAIL.setVolatile(this, 0);
        HEAD.setVolatile(this, 0);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected T getBufferElement(int index) {
        return (T) BUFFER_ARRAY.get(buffer, index);
    }

    @Override
    protected T getAndResetBufferElement(int index) {
        T e = getBufferElement(index);
        setBufferElement(index, null);
        return e;
    }

    @Override
    protected void setBufferElement(int index, T value) {
        BUFFER_ARRAY.set(buffer, index, value);
    }

    @Override
    public long getCurrentHead() {
        return (long) HEAD.getVolatile(this);
    }

    @Override
    public long getCurrentTail() {
        return (long) TAIL.getVolatile(this);
    }
}