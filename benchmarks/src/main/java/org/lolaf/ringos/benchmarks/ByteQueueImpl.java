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

import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;
import org.lolaf.ringos.rb.ByteFrameHandler;
import org.lolaf.ringos.rb.ByteRingBuffer;
import org.lolaf.ringos.rb.ByteRingBufferFactory;
import org.lolaf.ringos.rb.methodhandle.MethodHandleMpScByteRingBuffer;

import java.nio.ByteBuffer;

/**
 * The multi-producer, single-consumer byte queues compared: each writes a frame by copying it from a source buffer
 * of the payload's size, and reads by handing the frame to a handler that only notes its length.
 */
public enum ByteQueueImpl {

    /**
     * What the factory picks: the JDK 16+ implementation on this benchmark's JDK.
     */
    RINGOS_MPSC {
        @Override
        Adapter create(int capacity, int payload) {
            return ringos(ByteRingBufferFactory.queue(capacity), payload);
        }
    },
    /**
     * The Java 11 implementation, which cannot copy in bulk.
     */
    RINGOS_MPSC_JAVA11 {
        @Override
        Adapter create(int capacity, int payload) {
            return ringos(new MethodHandleMpScByteRingBuffer(capacity), payload);
        }
    },
    AGRONA_MANY_TO_ONE {
        @Override
        Adapter create(int capacity, int payload) {
            ManyToOneRingBuffer queue = new ManyToOneRingBuffer(
                    new UnsafeBuffer(ByteBuffer.allocateDirect(capacity + RingBufferDescriptor.TRAILER_LENGTH)));
            UnsafeBuffer src = new UnsafeBuffer(ByteBuffer.allocateDirect(payload));
            int[] lastLength = new int[1];
            MessageHandler handler = (type, buffer, index, length) -> lastLength[0] = length;
            return new Adapter() {
                @Override
                public boolean write() {
                    return queue.write(1, src, 0, payload);
                }

                @Override
                public int read(int limit) {
                    return queue.read(handler, limit) == 0 ? 0 : lastLength[0];
                }
            };
        }
    };

    abstract Adapter create(int capacity, int payload);

    private static Adapter ringos(ByteRingBuffer queue, int payload) {
        ByteBuffer src = ByteBuffer.allocateDirect(payload);
        int[] lastLength = new int[1];
        ByteFrameHandler handler = (type, buffer, index, length) -> lastLength[0] = length;
        return new Adapter() {
            @Override
            public boolean write() {
                return queue.write(1, src, 0, payload);
            }

            @Override
            public int read(int limit) {
                return queue.read(handler, limit) == 0 ? 0 : lastLength[0];
            }
        };
    }

    interface Adapter {

        /**
         * @return false when the queue is full
         */
        boolean write();

        /**
         * @return the length of the last frame read, 0 when none was
         */
        int read(int limit);
    }
}
