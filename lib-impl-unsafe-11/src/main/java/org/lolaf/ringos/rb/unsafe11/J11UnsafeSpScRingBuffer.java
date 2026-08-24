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

import org.lolaf.ringos.rb.spi.AbstractSpScRingBuffer;

import org.lolaf.ringos.unsafe.UnsafeOperations;
import org.lolaf.ringos.unsafe.UnsafeOperationsApi;

import java.util.function.IntFunction;

public class J11UnsafeSpScRingBuffer<T> extends AbstractSpScRingBuffer<T> {

    private static final jdk.internal.misc.Unsafe UNSAFE;
    private static final long SEQUENCE_ARRAY_BASE;
    private static final int SEQUENCE_ARRAY_SHIFT;
    private static final long BUFFER_ARRAY_BASE;
    private static final int BUFFER_ARRAY_SHIFT;

    static {
        UNSAFE = jdk.internal.misc.Unsafe.getUnsafe();
        UnsafeOperations unsafeOperations = UnsafeOperationsApi.get();
        SEQUENCE_ARRAY_BASE = unsafeOperations.arrayBaseOffset(long[].class);
        SEQUENCE_ARRAY_SHIFT = calculateShiftForScale(unsafeOperations.arrayIndexScale(long[].class));
        BUFFER_ARRAY_BASE = unsafeOperations.arrayBaseOffset(Object[].class);
        BUFFER_ARRAY_SHIFT = calculateShiftForScale(unsafeOperations.arrayIndexScale(Object[].class));
    }

    public J11UnsafeSpScRingBuffer(int capacity, boolean bufferPaddingEnabled, IntFunction<T> elementInstanceProducer) {
        super(capacity, bufferPaddingEnabled, elementInstanceProducer);
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
        setBufferElement(index, null);
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
        head = tail = 0;
    }

    @Override
    public long getCurrentHead() {
        return head;
    }

    @Override
    public long getCurrentTail() {
        return tail;
    }
}