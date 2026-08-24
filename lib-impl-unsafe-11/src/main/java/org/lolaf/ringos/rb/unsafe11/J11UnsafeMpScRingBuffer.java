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

import org.lolaf.ringos.rb.spi.AbstractSlotFlaggedMpScRingBuffer;

import org.lolaf.ringos.unsafe.UnsafeOperations;
import org.lolaf.ringos.unsafe.UnsafeOperationsApi;

/**
 * A slot-flagged multi-producer, single-consumer ring buffer over the {@code jdk.internal.misc.Unsafe} of
 * JDK 11 to 14, whose reference accessors are still named after {@code Object}.
 *
 * <p>See {@link AbstractSlotFlaggedMpScRingBuffer} for the protocol and for why this one carries no sequence
 * array. {@link J11UnsafePooledMpScRingBuffer} is the implementation to reach for when the slots must hold
 * pooled element instances.
 */
public class J11UnsafeMpScRingBuffer<T> extends AbstractSlotFlaggedMpScRingBuffer<T> {

    private static final jdk.internal.misc.Unsafe UNSAFE;
    private static final long HEAD_OFFSET;
    private static final long TAIL_OFFSET;
    private static final long BUFFER_ARRAY_BASE;
    private static final int BUFFER_ARRAY_SHIFT;

    static {
        try {
            UNSAFE = jdk.internal.misc.Unsafe.getUnsafe();
            UnsafeOperations unsafeOperations = UnsafeOperationsApi.get();
            HEAD_OFFSET = unsafeOperations.objectFieldOffset(AbstractRingBuffer.class.getDeclaredField("head"));
            TAIL_OFFSET = unsafeOperations.objectFieldOffset(AbstractRingBuffer.class.getDeclaredField("tail"));
            BUFFER_ARRAY_BASE = unsafeOperations.arrayBaseOffset(Object[].class);
            BUFFER_ARRAY_SHIFT = calculateShiftForScale(unsafeOperations.arrayIndexScale(Object[].class));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public J11UnsafeMpScRingBuffer(int capacity, boolean bufferPaddingEnabled) {
        super(capacity, bufferPaddingEnabled);
    }

    private static long bufferOffset(int index) {
        return BUFFER_ARRAY_BASE + ((long) index << BUFFER_ARRAY_SHIFT);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected T getBufferElementAcquire(int index) {
        return (T) UNSAFE.getObjectVolatile(buffer, bufferOffset(index));
    }

    @Override
    protected void setBufferElementRelease(int index, T value) {
        UNSAFE.putObjectRelease(buffer, bufferOffset(index), value);
    }

    @Override
    protected void clearBufferElementPlain(int index) {
        UNSAFE.putObject(buffer, bufferOffset(index), null);
    }

    @Override
    protected boolean tailCompareAndSwap(long current, long next) {
        return UNSAFE.compareAndSetLong(this, TAIL_OFFSET, current, next);
    }

    @Override
    protected void setHeadRelease(long value) {
        UNSAFE.putLongRelease(this, HEAD_OFFSET, value);
    }

    @Override
    protected void resetTailAndHead() {
        UNSAFE.putLongVolatile(this, TAIL_OFFSET, 0);
        UNSAFE.putLongVolatile(this, HEAD_OFFSET, 0);
    }

    @Override
    public long getCurrentHead() {
        return UNSAFE.getLongVolatile(this, HEAD_OFFSET);
    }

    @Override
    public long getCurrentTail() {
        return UNSAFE.getLongVolatile(this, TAIL_OFFSET);
    }
}
