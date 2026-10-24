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
package org.lolaf.ringos.rb.spi;

import jdk.internal.vm.annotation.Contended;
import org.lolaf.ringos.rb.ByteFrameHandler;
import org.lolaf.ringos.rb.ByteRingBuffer;
import org.lolaf.ringos.rb.ByteTranslator;

import java.nio.ByteBuffer;

/**
 * The claim protocol of a multi-producer, single-consumer {@link ByteRingBuffer}, after Agrona's
 * {@code ManyToOneRingBuffer}.
 *
 * <p>A frame is an 8-byte header, the record length then the type, followed by the payload, and starts on an 8-byte
 * boundary. The record length counts the header, so it is never 0 for a published frame and 0 can only mean "not
 * written yet". A producer claims space by advancing {@code tail}, marks the header with the negated length,
 * writes the type and payload, and publishes the positive length with a release store. The consumer stops at a
 * length that is not positive, hands the frame over, zeroes its bytes and only then releases {@code head}. Zeroing
 * is what keeps a stale byte of an old payload from reading as a published header once frames land on other
 * boundaries.
 *
 * <p>A frame that does not fit before the end of the buffer is preceded by a padding frame filling the rest, so a
 * frame is always contiguous. Producers check for room against {@link #headCache}, a lower bound on {@code head}
 * they share, and read the real one only when that says the buffer is full.
 *
 * <p>Header ints go through the implementation's accessors, in whatever byte order it chooses; the payload is the
 * caller's, written and read through the buffer in its own order.
 */
public abstract class AbstractMpScByteRingBuffer implements ByteRingBuffer {

    public static final int HEADER = 8;
    public static final int ALIGNMENT = 8;
    public static final int PADDING_TYPE = -1;
    protected static final int TYPE_OFFSET = 4;

    protected final ByteBuffer buffer;
    protected final int capacity;
    protected final int mask;
    protected final int maxPayloadLength;
    @Contended
    protected long head;
    @Contended
    protected long tail;
    @Contended
    private volatile long headCache;

    /**
     * @param buffer exactly {@code capacity} bytes, zeroed, with an address aligned to 8 so its header ints can be
     *               accessed atomically
     */
    protected AbstractMpScByteRingBuffer(ByteBuffer buffer, int capacity) {
        this.buffer = buffer;
        this.capacity = capacity;
        this.mask = capacity - 1;
        this.maxPayloadLength = capacity / 8 - HEADER;
    }

    /**
     * A zeroed direct buffer of exactly {@code capacity} bytes, its address aligned to a cache line.
     */
    protected static ByteBuffer newBuffer(int capacity) {
        ByteBuffer aligned = ByteBuffer.allocateDirect(capacity + 64).alignedSlice(64);
        aligned.limit(capacity);
        return aligned.slice();
    }

    protected abstract long getTailVolatile();

    protected abstract boolean tailCompareAndSwap(long current, long next);

    /**
     * Reads {@code head} with acquire semantics, so a producer that sees it advanced also sees the zeroed bytes.
     */
    protected abstract long getHeadAcquire();

    protected abstract void setHeadRelease(long value);

    protected abstract int getLengthAcquire(int index);

    protected abstract void setLengthRelease(int index, int length);

    protected abstract int getTypePlain(int index);

    protected abstract void setTypePlain(int index, int type);

    @Override
    public int getCapacity() {
        return capacity;
    }

    @Override
    public int getMaxPayloadLength() {
        return maxPayloadLength;
    }

    @Override
    public <A> boolean write(int type, int length, ByteTranslator<A> translator, A arg) {
        checkFrame(type, length, maxPayloadLength);
        int index = claim(type, HEADER + length);
        if (index < 0) {
            return false;
        }
        try {
            translator.translate(buffer, index + HEADER, length, arg);
        } catch (RuntimeException | Error e) {
            discard(index);
            throw e;
        }
        setLengthRelease(index, HEADER + length);
        return true;
    }

    @Override
    public boolean write(int type, ByteBuffer src, int srcIndex, int length) {
        checkFrame(type, length, maxPayloadLength);
        int index = claim(type, HEADER + length);
        if (index < 0) {
            return false;
        }
        copy(src, srcIndex, buffer, index + HEADER, length);
        setLengthRelease(index, HEADER + length);
        return true;
    }

    /**
     * Claims room for a record, behind a padding frame when it does not fit before the end, and marks its header
     * as being written.
     *
     * @return the index of the record's header, or -1 when the buffer is full
     */
    private int claim(int type, int record) {
        int frame = align(record);
        long claimed;
        int index;
        int padding;
        do {
            claimed = getTailVolatile();
            index = (int) (claimed & mask);
            int toEnd = capacity - index;
            padding = frame > toEnd ? toEnd : 0;
            long end = claimed + padding + frame;
            if (end - headCache > capacity) {
                long currentHead = getHeadAcquire();
                if (end - currentHead > capacity) {
                    return -1;
                }
                headCache = currentHead;
            }
        } while (!tailCompareAndSwap(claimed, claimed + padding + frame));

        if (padding > 0) {
            setTypePlain(index, PADDING_TYPE);
            setLengthRelease(index, padding);
            index = 0;
        }
        setLengthRelease(index, -record);
        setTypePlain(index, type);
        return index;
    }

    /**
     * Publishes a claimed record whose payload could not be written as padding, so the consumer skips it rather than
     * waiting on it forever.
     */
    private void discard(int index) {
        setTypePlain(index, PADDING_TYPE);
        setLengthRelease(index, -getLengthAcquire(index));
    }

    @Override
    public int read(ByteFrameHandler handler, int limit) {
        long consumed = head;
        int count = 0;
        try {
            while (count < limit) {
                int index = (int) (consumed & mask);
                int record = getLengthAcquire(index);
                if (record <= 0) {
                    break;
                }
                int frame = align(record);
                int type = getTypePlain(index);
                if (type != PADDING_TYPE) {
                    handler.onFrame(type, buffer, index + HEADER, record - HEADER);
                    count++;
                }
                zero(index, frame);
                consumed += frame;
            }
        } finally {
            if (consumed != head) {
                head = consumed;
                setHeadRelease(consumed);
            }
        }
        return count;
    }

    @Override
    public int getSize() {
        return (int) (getTailVolatile() - getHeadAcquire());
    }

    private void zero(int index, int frame) {
        for (int i = 0; i < frame; i += Long.BYTES) {
            buffer.putLong(index + i, 0L);
        }
    }

    protected static int align(int length) {
        return (length + ALIGNMENT - 1) & -ALIGNMENT;
    }

    protected static void checkFrame(int type, int length, int maxPayloadLength) {
        if (type < 0) {
            throw new IllegalArgumentException("Type must be at least 0, got " + type);
        }
        if (length < 0 || length > maxPayloadLength) {
            throw new IllegalArgumentException("Payload length must be 0.." + maxPayloadLength + ", got " + length);
        }
    }

    /**
     * Copies without touching either buffer's position, which an absolute bulk copy cannot do before Java 16; by longs
     * only when both buffers read them in the same byte order.
     */
    protected static void copy(ByteBuffer src, int srcIndex, ByteBuffer dst, int dstIndex, int length) {
        int i = 0;
        if (src.order() == dst.order()) {
            for (; i + Long.BYTES <= length; i += Long.BYTES) {
                dst.putLong(dstIndex + i, src.getLong(srcIndex + i));
            }
        }
        for (; i < length; i++) {
            dst.put(dstIndex + i, src.get(srcIndex + i));
        }
    }
}
