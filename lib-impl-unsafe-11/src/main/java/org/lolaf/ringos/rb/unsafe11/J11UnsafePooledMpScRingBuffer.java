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

import org.lolaf.ringos.rb.spi.AbstractRingBuffer;

import org.lolaf.ringos.rb.spi.AbstractMpMcRingBuffer;

import org.lolaf.ringos.unsafe.UnsafeOperations;
import org.lolaf.ringos.unsafe.UnsafeOperationsApi;

import java.util.function.Consumer;
import java.util.function.IntFunction;

public class J11UnsafePooledMpScRingBuffer<T> extends AbstractMpMcRingBuffer<T> {

    private static final jdk.internal.misc.Unsafe UNSAFE;
    private static final long TAIL_OFFSET;
    private static final long SEQUENCE_ARRAY_BASE;
    private static final int SEQUENCE_ARRAY_SHIFT;
    private static final long BUFFER_ARRAY_BASE;
    private static final int BUFFER_ARRAY_SHIFT;

    static {
        try {
            UNSAFE = jdk.internal.misc.Unsafe.getUnsafe();
            UnsafeOperations unsafeOperations = UnsafeOperationsApi.get();
            TAIL_OFFSET = unsafeOperations.objectFieldOffset(AbstractRingBuffer.class.getDeclaredField("tail"));
            SEQUENCE_ARRAY_BASE = unsafeOperations.arrayBaseOffset(long[].class);
            SEQUENCE_ARRAY_SHIFT = calculateShiftForScale(unsafeOperations.arrayIndexScale(long[].class));
            BUFFER_ARRAY_BASE = unsafeOperations.arrayBaseOffset(Object[].class);
            BUFFER_ARRAY_SHIFT = calculateShiftForScale(unsafeOperations.arrayIndexScale(Object[].class));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public J11UnsafePooledMpScRingBuffer(int capacity, boolean bufferPaddingEnabled, IntFunction<T> elementInstanceProducer) {
        super(capacity, bufferPaddingEnabled, elementInstanceProducer);
    }

    @Override
    public T poll() {
        long currentHead = head;
        long nextHead = currentHead + 1;
        int index = getIndex(currentHead);
        if (getSequence(index) < nextHead) {
            return null;
        }

        head = nextHead;
        T item = clearElementOnPoll ? getAndResetBufferElement(index) : getBufferElement(index);
        setSequence(index, nextHead + mask);
        return item;
    }

    @Override
    public boolean poll(Consumer<T> consumer) {
        long currentHead = head;
        long nextHead = currentHead + 1;
        int index = getIndex(currentHead);
        if (getSequence(index) < nextHead) {
            return false;
        }

        head = nextHead;
        T item = clearElementOnPoll ? getAndResetBufferElement(index) : getBufferElement(index);
        consumer.accept(item);
        setSequence(index, nextHead + mask);
        return true;
    }

    @Override
    public int poll(Consumer<T>[] consumers) {
        long currentHead = getCurrentHead();
        int available = getAvailableElementsToRead(consumers.length, currentHead);
        if (available == 0) {
            return 0;
        }
        head = currentHead + available;
        for (int i = 0; i < available; i++) {
            long headIndex = currentHead + i;
            int index = getIndex(headIndex);
            T item = clearElementOnPoll ? getAndResetBufferElement(index) : getBufferElement(index);
            consumers[i].accept(item);
            setSequence(index, headIndex + capacity);
        }
        return available;
    }

    @Override
    protected boolean headCompareAndSwap(long current, long next) {
        throw new IllegalStateException("Should never haven been called");
    }

    @Override
    protected boolean tailCompareAndSwap(long current, long next) {
        return UNSAFE.compareAndSetLong(this, TAIL_OFFSET, current, next);
    }

    private static long bufferOffset(int index) {
        return BUFFER_ARRAY_BASE + ((long) index << BUFFER_ARRAY_SHIFT);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected T getBufferElement(int index) {
        return (T) UNSAFE.getObjectVolatile(buffer, bufferOffset(index));
    }

    @Override
    protected T getAndResetBufferElement(int index) {
        T e = getBufferElement(index);
        UNSAFE.putObject(buffer, bufferOffset(index), null);
        return e;
    }

    @Override
    protected void setBufferElement(int index, T value) {
        UNSAFE.putObjectRelease(buffer, bufferOffset(index), value);
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
        UNSAFE.putLongVolatile(this, TAIL_OFFSET, 0);
        head = 0;
    }

    @Override
    public long getCurrentHead() {
        return head;
    }

    @Override
    public long getCurrentTail() {
        return UNSAFE.getLongVolatile(this, TAIL_OFFSET);
    }
}