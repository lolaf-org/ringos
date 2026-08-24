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
package org.lolaf.ringos.rb.unsafe;

import org.lolaf.ringos.rb.spi.AbstractRingBuffer;

import org.lolaf.ringos.rb.spi.AbstractMpMcRingBuffer;

import org.lolaf.ringos.unsafe.UnsafeOperations;
import org.lolaf.ringos.unsafe.UnsafeOperationsApi;

import java.util.function.IntFunction;

public class UnsafeSpMcRingBuffer<T> extends AbstractMpMcRingBuffer<T> {

    private static final jdk.internal.misc.Unsafe UNSAFE;
    private static final long HEAD_OFFSET;
    private static final long SEQUENCE_ARRAY_BASE;
    private static final int SEQUENCE_ARRAY_SHIFT;
    private static final long BUFFER_ARRAY_BASE;
    private static final int BUFFER_ARRAY_SHIFT;

    static {
        try {
            UNSAFE = jdk.internal.misc.Unsafe.getUnsafe();
            UnsafeOperations unsafeOperations = UnsafeOperationsApi.get();
            HEAD_OFFSET = unsafeOperations.objectFieldOffset(AbstractRingBuffer.class.getDeclaredField("head"));
            SEQUENCE_ARRAY_BASE = unsafeOperations.arrayBaseOffset(long[].class);
            SEQUENCE_ARRAY_SHIFT = calculateShiftForScale(unsafeOperations.arrayIndexScale(long[].class));
            BUFFER_ARRAY_BASE = unsafeOperations.arrayBaseOffset(Object[].class);
            BUFFER_ARRAY_SHIFT = calculateShiftForScale(unsafeOperations.arrayIndexScale(Object[].class));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public UnsafeSpMcRingBuffer(int capacity, boolean bufferPaddingEnabled, IntFunction<T> elementInstanceProducer) {
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
    protected boolean headCompareAndSwap(long current, long next) {
        return UNSAFE.compareAndSetLong(this, HEAD_OFFSET, current, next);
    }

    @Override
    protected boolean tailCompareAndSwap(long current, long next) {
        throw new IllegalStateException("Should never haven been called");
    }

    private static long bufferOffset(int index) {
        return BUFFER_ARRAY_BASE + ((long) index << BUFFER_ARRAY_SHIFT);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected T getBufferElement(int index) {
        return (T) UNSAFE.getReferenceVolatile(buffer, bufferOffset(index));
    }

    @Override
    protected T getAndResetBufferElement(int index) {
        T e = getBufferElement(index);
        UNSAFE.putReference(buffer, bufferOffset(index), null);
        return e;
    }

    @Override
    protected void setBufferElement(int index, T value) {
        UNSAFE.putReferenceRelease(buffer, bufferOffset(index), value);
    }

    private static long sequenceOffset(int index) {
        return SEQUENCE_ARRAY_BASE + ((long) index << SEQUENCE_ARRAY_SHIFT);
    }

    @Override
    protected long getSequence(int index) {
        return UNSAFE.getLongVolatile(sequences, sequenceOffset(index));
    }

    @Override
    protected void setSequence(int index, long value) {
        UNSAFE.putLongRelease(sequences, sequenceOffset(index), value);
    }

    @Override
    protected void resetTailAndHead() {
        tail = 0;
        UNSAFE.putLongVolatile(this, HEAD_OFFSET, 0);
    }

    @Override
    public long getCurrentHead() {
        return UNSAFE.getLongVolatile(this, HEAD_OFFSET);
    }

    @Override
    public long getCurrentTail() {
        return tail;
    }
}