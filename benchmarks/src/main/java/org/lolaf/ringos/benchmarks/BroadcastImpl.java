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
package org.lolaf.ringos.benchmarks;

import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.broadcast.BroadcastBufferDescriptor;
import org.agrona.concurrent.broadcast.BroadcastReceiver;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;
import org.lolaf.ringos.rb.BroadcastByteRingBuffer;
import org.lolaf.ringos.rb.ByteRingBufferFactory;
import org.lolaf.ringos.rb.ByteRingReader;
import org.lolaf.ringos.rb.methodhandle.MethodHandleSpBroadcastByteRingBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The broadcast buffers compared. Writes copy the frame from a source buffer of the payload's size; reads copy it into
 * a destination and then check it was not overwritten, which is how a ringos reader works and how an Agrona
 * {@code BroadcastReceiver} is meant to be used ({@code receiveNext}, copy, {@code validate}).
 */
public enum BroadcastImpl {

    /**
     * What the factory picks: the JDK 16+ implementation on this benchmark's JDK.
     */
    RINGOS_SINGLE(false) {
        @Override
        Adapter create(int capacity, int payload) {
            return ringos(ByteRingBufferFactory.broadcast(capacity, BroadcastByteRingBuffer.Producers.SINGLE), payload);
        }
    },
    /**
     * The Java 11 implementation, which cannot copy in bulk.
     */
    RINGOS_SINGLE_JAVA11(false) {
        @Override
        Adapter create(int capacity, int payload) {
            return ringos(new MethodHandleSpBroadcastByteRingBuffer(capacity), payload);
        }
    },
    RINGOS_MULTI(true) {
        @Override
        Adapter create(int capacity, int payload) {
            return ringos(ByteRingBufferFactory.broadcast(capacity, BroadcastByteRingBuffer.Producers.MULTI), payload);
        }
    },
    AGRONA(false) {
        @Override
        Adapter create(int capacity, int payload) {
            return agrona(capacity, payload, false);
        }
    },
    /**
     * Agrona has one writer; this is it behind the same spin lock as {@link #RINGOS_MULTI}, to compare like for like.
     */
    AGRONA_SPIN_LOCKED(true) {
        @Override
        Adapter create(int capacity, int payload) {
            return agrona(capacity, payload, true);
        }
    };

    final boolean multiProducer;

    BroadcastImpl(boolean multiProducer) {
        this.multiProducer = multiProducer;
    }

    abstract Adapter create(int capacity, int payload);

    private static Adapter ringos(BroadcastByteRingBuffer ring, int payload) {
        ByteBuffer src = ByteBuffer.allocateDirect(payload);
        ByteBuffer dst = ByteBuffer.allocateDirect(payload);
        ByteRingReader reader = ring.newReader();
        return new Adapter() {
            @Override
            public void write() {
                ring.write(1, src, 0, payload);
            }

            @Override
            public int read() {
                int length = reader.readInto(dst, 0);
                if (length == ByteRingReader.LAPPED) {
                    reader.seekToTail();
                }
                return length;
            }
        };
    }

    private static Adapter agrona(int capacity, int payload, boolean locked) {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(capacity + BroadcastBufferDescriptor.TRAILER_LENGTH));
        BroadcastTransmitter transmitter = new BroadcastTransmitter(buffer);
        BroadcastReceiver receiver = new BroadcastReceiver(buffer);
        UnsafeBuffer src = new UnsafeBuffer(ByteBuffer.allocateDirect(payload));
        UnsafeBuffer dst = new UnsafeBuffer(ByteBuffer.allocateDirect(payload));
        AtomicBoolean lock = new AtomicBoolean();
        return new Adapter() {
            @Override
            public void write() {
                if (!locked) {
                    transmitter.transmit(1, src, 0, payload);
                    return;
                }
                while (!lock.compareAndSet(false, true)) {
                    Thread.onSpinWait();
                }
                try {
                    transmitter.transmit(1, src, 0, payload);
                } finally {
                    lock.set(false);
                }
            }

            @Override
            public int read() {
                if (!receiver.receiveNext()) {
                    return ByteRingReader.NO_FRAME;
                }
                int length = receiver.length();
                dst.putBytes(0, receiver.buffer(), receiver.offset(), length);
                return receiver.validate() ? length : ByteRingReader.LAPPED;
            }
        };
    }

    interface Adapter {

        void write();

        /**
         * @return the payload length, or {@link ByteRingReader#NO_FRAME} or {@link ByteRingReader#LAPPED}
         */
        int read();
    }
}
