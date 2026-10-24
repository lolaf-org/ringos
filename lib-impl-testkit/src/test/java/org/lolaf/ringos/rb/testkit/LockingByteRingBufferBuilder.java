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
package org.lolaf.ringos.rb.testkit;

import org.lolaf.ringos.rb.BroadcastByteRingBuffer;
import org.lolaf.ringos.rb.ByteFrameHandler;
import org.lolaf.ringos.rb.ByteRingBuffer;
import org.lolaf.ringos.rb.ByteRingBufferBuilder;
import org.lolaf.ringos.rb.ByteRingReader;
import org.lolaf.ringos.rb.ByteTranslator;
import org.lolaf.ringos.rb.ProducerType;

import java.nio.ByteBuffer;

/**
 * The byte ring buffers as simply as they can be written: one lock, the layout the real implementations use. It
 * runs the kit in this module, so the tests are known to be right before a lock-free implementation meets them.
 */
public class LockingByteRingBufferBuilder implements ByteRingBufferBuilder {

    static final int PADDING = -1;

    private static int align(int length, int alignment) {
        return (length + alignment - 1) & -alignment;
    }

    private static void checkFrame(int type, int length, int maxPayloadLength) {
        if (type < 0) {
            throw new IllegalArgumentException("Type must be at least 0, got " + type);
        }
        if (length < 0 || length > maxPayloadLength) {
            throw new IllegalArgumentException("Payload length must be 0.." + maxPayloadLength + ", got " + length);
        }
    }

    private static void copy(ByteBuffer src, int srcIndex, ByteBuffer dst, int dstIndex, int length) {
        for (int i = 0; i < length; i++) {
            dst.put(dstIndex + i, src.get(srcIndex + i));
        }
    }

    @Override
    public ByteRingBuffer buildQueue(int capacity) {
        return new Queue(capacity);
    }

    @Override
    public BroadcastByteRingBuffer buildBroadcast(int capacity, ProducerType producers) {
        return new Broadcast(capacity);
    }

    /**
     * Header: payload length 4, type 4. Frames aligned to 8.
     */
    private static final class Queue implements ByteRingBuffer {

        private static final int HEADER = 8;

        private final ByteBuffer buffer;
        private final int capacity;
        private long head;
        private long tail;

        Queue(int capacity) {
            this.capacity = capacity;
            this.buffer = ByteBuffer.allocateDirect(capacity);
        }

        @Override
        public int getCapacity() {
            return capacity;
        }

        @Override
        public int getMaxPayloadLength() {
            return capacity / 8 - HEADER;
        }

        @Override
        public synchronized <A> boolean write(int type, int length, ByteTranslator<A> translator, A arg) {
            checkFrame(type, length, getMaxPayloadLength());
            int frame = align(HEADER + length, HEADER);
            int index = (int) (tail & (capacity - 1));
            int toEnd = capacity - index;
            int padding = frame > toEnd ? toEnd : 0;
            if (tail + padding + frame - head > capacity) {
                return false;
            }
            if (padding > 0) {
                buffer.putInt(index, padding - HEADER);
                buffer.putInt(index + 4, PADDING);
                tail += padding;
                index = 0;
            }
            buffer.putInt(index, length);
            buffer.putInt(index + 4, type);
            translator.translate(buffer, index + HEADER, length, arg);
            tail += frame;
            return true;
        }

        @Override
        public boolean write(int type, ByteBuffer src, int srcIndex, int length) {
            return write(type, length, (buffer, index, l, s) -> copy(s, srcIndex, buffer, index, l), src);
        }

        @Override
        public synchronized int read(ByteFrameHandler handler, int limit) {
            int count = 0;
            while (count < limit && head < tail) {
                int index = (int) (head & (capacity - 1));
                int length = buffer.getInt(index);
                int type = buffer.getInt(index + 4);
                if (type != PADDING) {
                    handler.onFrame(type, buffer, index + HEADER, length);
                    count++;
                }
                head += align(HEADER + length, HEADER);
            }
            return count;
        }

        @Override
        public synchronized int getSize() {
            return (int) (tail - head);
        }
    }

    /**
     * Header: payload length 4, type 4, position 8. Frames aligned to 16, so a padding header always fits.
     */
    private static final class Broadcast implements BroadcastByteRingBuffer {

        private static final int HEADER = 16;

        private final ByteBuffer buffer;
        private final int capacity;
        private long tail;

        Broadcast(int capacity) {
            this.capacity = capacity;
            this.buffer = ByteBuffer.allocateDirect(capacity);
        }

        @Override
        public int getCapacity() {
            return capacity;
        }

        @Override
        public int getMaxPayloadLength() {
            return capacity / 8 - HEADER;
        }

        @Override
        public synchronized <A> long write(int type, int length, ByteTranslator<A> translator, A arg) {
            checkFrame(type, length, getMaxPayloadLength());
            int frame = align(HEADER + length, HEADER);
            int index = (int) (tail & (capacity - 1));
            int toEnd = capacity - index;
            if (frame > toEnd) {
                header(index, toEnd - HEADER, PADDING, tail);
                tail += toEnd;
                index = 0;
            }
            long position = tail;
            header(index, length, type, position);
            translator.translate(buffer, index + HEADER, length, arg);
            tail += frame;
            return position;
        }

        private void header(int index, int length, int type, long position) {
            buffer.putInt(index, length);
            buffer.putInt(index + 4, type);
            buffer.putLong(index + 8, position);
        }

        @Override
        public long write(int type, ByteBuffer src, int srcIndex, int length) {
            return write(type, length, (buffer, index, l, s) -> copy(s, srcIndex, buffer, index, l), src);
        }

        @Override
        public synchronized long getTailPosition() {
            return tail;
        }

        @Override
        public synchronized ByteRingReader newReader() {
            return new Reader(this, tail);
        }
    }

    private static final class Reader implements ByteRingReader {

        private final Broadcast ring;
        private long position;
        private int type;
        private long framePosition = -1;

        Reader(Broadcast ring, long position) {
            this.ring = ring;
            this.position = position;
        }

        @Override
        public long getPosition() {
            return position;
        }

        @Override
        public int readInto(ByteBuffer dst, int dstIndex) {
            synchronized (ring) {
                while (true) {
                    if (position == ring.tail) {
                        return NO_FRAME;
                    }
                    if (position < ring.tail - ring.capacity) {
                        return LAPPED;
                    }
                    int index = (int) (position & (ring.capacity - 1));
                    int length = ring.buffer.getInt(index);
                    int frameType = ring.buffer.getInt(index + 4);
                    int frame = align(Broadcast.HEADER + length, Broadcast.HEADER);
                    if (frameType == PADDING) {
                        position += frame;
                        continue;
                    }
                    if (dst.limit() - dstIndex < length) {
                        throw new IllegalArgumentException("Frame of " + length + " bytes, room for " + (dst.limit() - dstIndex));
                    }
                    copy(ring.buffer, index + Broadcast.HEADER, dst, dstIndex, length);
                    type = frameType;
                    framePosition = position;
                    position += frame;
                    return length;
                }
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
            synchronized (ring) {
                if (target == ring.tail) {
                    position = target;
                    return true;
                }
                if (target > ring.tail || target < ring.tail - ring.capacity || target % Broadcast.HEADER != 0) {
                    return false;
                }
                if (ring.buffer.getLong((int) (target & (ring.capacity - 1)) + 8) != target) {
                    return false;
                }
                position = target;
                return true;
            }
        }

        @Override
        public void seekToTail() {
            synchronized (ring) {
                position = ring.tail;
            }
        }
    }
}
