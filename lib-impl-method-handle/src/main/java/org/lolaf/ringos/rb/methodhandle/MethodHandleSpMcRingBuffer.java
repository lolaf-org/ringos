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

public class MethodHandleSpMcRingBuffer<T> extends AbstractMpMcRingBuffer<T> {

    private static final VarHandle HEAD;
    private static final VarHandle SEQUENCE_ARRAY;
    private static final VarHandle BUFFER_ARRAY;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            HEAD = lookup.findVarHandle(MethodHandleSpMcRingBuffer.class, "head", long.class);
            SEQUENCE_ARRAY = MethodHandles.arrayElementVarHandle(long[].class);
            BUFFER_ARRAY = MethodHandles.arrayElementVarHandle(Object[].class);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    public MethodHandleSpMcRingBuffer(int capacity, boolean bufferPaddingEnabled, IntFunction<T> elementInstanceProducer) {
        super(capacity, bufferPaddingEnabled, elementInstanceProducer);
    }


    @Override
    public boolean offer(T item) {
        requireStorableElement(item);
        long currentTail = getCurrentTail();
        long nextTail = currentTail + 1;
        int index = getIndex(currentTail);
        if (getSequence(index) < currentTail) {
            return false;
        }

        tail = nextTail;
        setBufferElement(index, item);
        setSequence(index, nextTail);
        return true;
    }

    @Override
    public <A> boolean offer(EventTranslatorOneArg<T, A> eventTranslator, A arg1) {
        long currentTail = getCurrentTail();
        long nextTail = currentTail + 1;
        int index = getIndex(currentTail);
        if (getSequence(index) < currentTail) {
            return false;
        }

        tail = nextTail;
        eventTranslator.translate(getBufferElement(index), arg1);
        setSequence(index, nextTail);
        return true;
    }

    @Override
    public <A, B> boolean offer(EventTranslatorTwoArg<T, A, B> eventTranslator, A arg1, B arg2) {
        long currentTail = getCurrentTail();
        long nextTail = currentTail + 1;
        int index = getIndex(currentTail);
        if (getSequence(index) < currentTail) {
            return false;
        }

        tail = nextTail;
        eventTranslator.translate(getBufferElement(index), arg1, arg2);
        setSequence(index, nextTail);
        return true;
    }

    @Override
    public <A, B, C> boolean offer(EventTranslatorThreeArg<T, A, B, C> eventTranslator, A arg1, B arg2, C arg3) {
        long currentTail = getCurrentTail();
        long nextTail = currentTail + 1;
        int index = getIndex(currentTail);
        if (getSequence(index) < currentTail) {
            return false;
        }

        tail = nextTail;
        eventTranslator.translate(getBufferElement(index), arg1, arg2, arg3);
        setSequence(index, nextTail);
        return true;
    }

    @Override
    public <A, B> boolean offer(EventTranslatorThreeLongArg<T, A, B> eventTranslator, long arg1, A arg2, B arg3) {
        long currentTail = getCurrentTail();
        long nextTail = currentTail + 1;
        int index = getIndex(currentTail);
        if (getSequence(index) < currentTail) {
            return false;
        }

        tail = nextTail;
        eventTranslator.translate(getBufferElement(index), arg1, arg2, arg3);
        setSequence(index, nextTail);
        return true;
    }

    @Override
    public <A, B, C, D> boolean offer(EventTranslatorFourArg<T, A, B, C, D> eventTranslator, A arg1, B arg2, C arg3, D arg4) {
        long currentTail = getCurrentTail();
        long nextTail = currentTail + 1;
        int index = getIndex(currentTail);
        if (getSequence(index) < currentTail) {
            return false;
        }

        tail = nextTail;
        eventTranslator.translate(getBufferElement(index), arg1, arg2, arg3, arg4);
        setSequence(index, nextTail);
        return true;
    }

    @Override
    public <A, B, C, D, E> boolean offer(EventTranslatorFiveArg<T, A, B, C, D, E> eventTranslator, A arg1, B arg2, C arg3, D arg4, E arg5) {
        long currentTail = getCurrentTail();
        long nextTail = currentTail + 1;
        int index = getIndex(currentTail);
        if (getSequence(index) < currentTail) {
            return false;
        }

        tail = nextTail;
        eventTranslator.translate(getBufferElement(index), arg1, arg2, arg3, arg4, arg5);
        setSequence(index, nextTail);
        return true;
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
        throw new IllegalStateException("Should never haven been called");
    }

    @Override
    protected void resetTailAndHead() {
        tail = 0;
        HEAD.setVolatile(this, 0);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected T getBufferElement(int index) {
        return (T) BUFFER_ARRAY.getVolatile(buffer, index);
    }

    @Override
    protected T getAndResetBufferElement(int index) {
        T e = getBufferElement(index);
        BUFFER_ARRAY.set(buffer, index, null);
        return e;
    }

    @Override
    protected void setBufferElement(int index, T value) {
        BUFFER_ARRAY.setRelease(buffer, index, value);
    }

    @Override
    public long getCurrentHead() {
        return (long) HEAD.getVolatile(this);
    }

    @Override
    public long getCurrentTail() {
        return tail;
    }
}