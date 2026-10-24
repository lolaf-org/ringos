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
import org.lolaf.ringos.rb.BroadcastByteRingBuffer;
import org.lolaf.ringos.rb.ByteRingReader;
import org.lolaf.ringos.rb.ByteTranslator;

import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;

/**
 * The protocol of a {@link BroadcastByteRingBuffer} with one writer at a time, after Agrona's
 * {@code BroadcastTransmitter}; {@link Producers#MULTI} implementations hold {@link #writeLock} around each write.
 *
 * <p>A frame is a 16-byte header, the record length (header plus payload), the type and the frame's position, then
 * the payload, on a 16-byte boundary so a padding header always fits before the end. The writer first raises
 * {@code tailIntent} to the end of what it is about to write, fences, writes the frame, and only then releases
 * {@code tail}. A reader copies a frame, fences, and re-reads {@code tailIntent}: if a writer may have reached the
 * frame's bytes during the copy, the frame is reported {@link ByteRingReader#LAPPED} rather than handed over. The
 * position in the header tells a frame from the one of a previous lap at the same index.
 */
public abstract class AbstractBroadcastByteRingBuffer implements BroadcastByteRingBuffer {

    public static final int HEADER = 16;
    public static final int ALIGNMENT = 16;
    public static final int PADDING_TYPE = -1;
    protected static final int TYPE_OFFSET = 4;
    protected static final int POSITION_OFFSET = 8;

    protected final ByteBuffer buffer;
    protected final int capacity;
    protected final int mask;
    protected final int maxPayloadLength;
    @Contended
    protected long tail;
    @Contended
    protected long tailIntent;
    /**
     * Held by the writer of a {@link Producers#MULTI} buffer; unused otherwise.
     */
    @Contended
    protected int writeLock;

    /**
     * @param buffer exactly {@code capacity} bytes with an address aligned to 8, so its header fields can be accessed
     *               atomically
     */
    protected AbstractBroadcastByteRingBuffer(ByteBuffer buffer, int capacity) {
        this.buffer = buffer;
        this.capacity = capacity;
        this.mask = capacity - 1;
        this.maxPayloadLength = capacity / 8 - HEADER;
    }

    protected abstract long getTailAcquire();

    protected abstract void setTailRelease(long value);

    protected abstract long getTailIntentAcquire();

    protected abstract void setTailIntentRelease(long value);

    protected abstract int getIntPlain(int index);

    protected abstract void setIntPlain(int index, int value);

    protected abstract long getLongPlain(int index);

    protected abstract void setLongPlain(int index, long value);

    @Override
    public int getCapacity() {
        return capacity;
    }

    @Override
    public int getMaxPayloadLength() {
        return maxPayloadLength;
    }

    @Override
    public <A> long write(int type, int length, ByteTranslator<A> translator, A arg) {
        AbstractMpScByteRingBuffer.checkFrame(type, length, maxPayloadLength);
        long position = begin(type, length);
        try {
            translator.translate(buffer, index(position) + HEADER, length, arg);
        } catch (RuntimeException | Error e) {
            setIntPlain(index(position) + TYPE_OFFSET, PADDING_TYPE);
            end(position, length);
            throw e;
        }
        end(position, length);
        return position;
    }

    @Override
    public long write(int type, ByteBuffer src, int srcIndex, int length) {
        AbstractMpScByteRingBuffer.checkFrame(type, length, maxPayloadLength);
        long position = begin(type, length);
        AbstractMpScByteRingBuffer.copy(src, srcIndex, buffer, index(position) + HEADER, length);
        end(position, length);
        return position;
    }

    /**
     * Announces the bytes about to be written, writes the padding before the frame if it needs any, and the frame's
     * header.
     *
     * @return the frame's position
     */
    private long begin(int type, int length) {
        long position = tail;
        int frame = align(HEADER + length);
        int index = index(position);
        int toEnd = capacity - index;
        int padding = frame > toEnd ? toEnd : 0;
        setTailIntentRelease(position + padding + frame);
        VarHandle.storeStoreFence();
        if (padding > 0) {
            header(index, padding, PADDING_TYPE, position);
            position += padding;
        }
        header(index(position), HEADER + length, type, position);
        return position;
    }

    private void end(long position, int length) {
        setTailRelease(position + align(HEADER + length));
    }

    private void header(int index, int record, int type, long position) {
        setIntPlain(index, record);
        setIntPlain(index + TYPE_OFFSET, type);
        setLongPlain(index + POSITION_OFFSET, position);
    }

    @Override
    public long getTailPosition() {
        return getTailAcquire();
    }

    @Override
    public ByteRingReader newReader() {
        return new Reader(getTailAcquire());
    }

    protected int index(long position) {
        return (int) (position & mask);
    }

    protected static int align(int length) {
        return (length + ALIGNMENT - 1) & -ALIGNMENT;
    }

    /**
     * Whether a writer may have reached the bytes from {@code position} on; read after the bytes it vouches for.
     */
    private boolean overwritten(long position) {
        VarHandle.acquireFence();
        return getTailIntentAcquire() - capacity > position;
    }

    private final class Reader implements ByteRingReader {

        private long position;
        private int type;
        private long framePosition = -1;

        Reader(long position) {
            this.position = position;
        }

        @Override
        public long getPosition() {
            return position;
        }

        @Override
        public int readInto(ByteBuffer dst, int dstIndex) {
            while (true) {
                if (position >= getTailAcquire()) {
                    return NO_FRAME;
                }
                if (getTailIntentAcquire() - capacity > position) {
                    return LAPPED;
                }
                int index = index(position);
                int record = getIntPlain(index);
                int frameType = getIntPlain(index + TYPE_OFFSET);
                long tag = getLongPlain(index + POSITION_OFFSET);
                if (tag != position || record < HEADER || record > HEADER + maxPayloadLength && frameType != PADDING_TYPE) {
                    return LAPPED;
                }
                if (frameType == PADDING_TYPE) {
                    if (overwritten(position)) {
                        return LAPPED;
                    }
                    position += align(record);
                    continue;
                }
                int length = record - HEADER;
                if (dst.limit() - dstIndex < length) {
                    if (overwritten(position)) {
                        return LAPPED;
                    }
                    throw new IllegalArgumentException("Frame of " + length + " bytes, room for " + (dst.limit() - dstIndex));
                }
                AbstractMpScByteRingBuffer.copy(buffer, index + HEADER, dst, dstIndex, length);
                if (overwritten(position)) {
                    return LAPPED;
                }
                type = frameType;
                framePosition = position;
                position += align(record);
                return length;
            }
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public long getFramePosition() {
            return framePosition;
        }

        @Override
        public boolean seek(long target) {
            long published = getTailAcquire();
            if (target == published) {
                position = target;
                return true;
            }
            if (target > published || target % ALIGNMENT != 0 || getTailIntentAcquire() - capacity > target) {
                return false;
            }
            long tag = getLongPlain(index(target) + POSITION_OFFSET);
            if (tag != target || overwritten(target)) {
                return false;
            }
            position = target;
            return true;
        }

        @Override
        public void seekToTail() {
            position = getTailAcquire();
        }
    }
}
