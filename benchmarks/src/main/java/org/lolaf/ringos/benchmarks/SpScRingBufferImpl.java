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

import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.jctools.queues.SpscArrayQueue;
import org.lolaf.ringos.rb.methodhandle.MethodHandleSpScRingBuffer;
import org.lolaf.ringos.rb.unsafe.UnsafeSpScRingBuffer;

/**
 * The implementations measured under a single-producer / single-consumer topology.
 *
 * <p>The single-producer, single-consumer implementations are the subject of the comparison; the
 * many-to-many entries delegate to {@link MpMcRingBufferImpl} and are measured as a baseline, so the cost of
 * running a general queue on the narrowest topology can be read off directly. Note that the third-party
 * entries here are each library's own one-to-one queue, not the many-to-many queue of the same library —
 * comparing a specialised implementation against a general one measures the specialisation rather than the
 * library.
 */
public enum SpScRingBufferImpl {
    METHOD_HANDLE_API_SPSC {
        @Override
        <T> RingBufferInterface<T> create(int capacity) {
            return new RingBufferAdapter<>(new MethodHandleSpScRingBuffer<>(capacity, false, null));
        }
    },
    METHOD_HANDLE_API_SPSC_PADDED {
        @Override
        <T> RingBufferInterface<T> create(int capacity) {
            return new RingBufferAdapter<>(new MethodHandleSpScRingBuffer<>(capacity, true, null));
        }
    },
    UNSAFE_API_SPSC {
        @Override
        <T> RingBufferInterface<T> create(int capacity) {
            return new RingBufferAdapter<>(new UnsafeSpScRingBuffer<>(capacity, false, null));
        }
    },
    UNSAFE_API_SPSC_PADDED {
        @Override
        <T> RingBufferInterface<T> create(int capacity) {
            return new RingBufferAdapter<>(new UnsafeSpScRingBuffer<>(capacity, true, null));
        }
    },
    JCT_SPSC_QUEUE {
        @Override
        <T> RingBufferInterface<T> create(int capacity) {
            return new JCTSpScQueueAdapter<>(capacity);
        }
    },
    AGRONA_SPSC_QUEUE {
        @Override
        <T> RingBufferInterface<T> create(int capacity) {
            return new AgronaSpScQueueAdapter<>(capacity);
        }
    },
    METHOD_HANDLE_API_MPMC {
        @Override
        <T> RingBufferInterface<T> create(int capacity) {
            return MpMcRingBufferImpl.METHOD_HANDLE_API.create(capacity);
        }
    },
    UNSAFE_API_MPMC {
        @Override
        <T> RingBufferInterface<T> create(int capacity) {
            return MpMcRingBufferImpl.UNSAFE_API.create(capacity);
        }
    },
    JCT_MPMC_QUEUE {
        @Override
        <T> RingBufferInterface<T> create(int capacity) {
            return MpMcRingBufferImpl.JCT_QUEUE.create(capacity);
        }
    },
    AGRONA_MPMC_QUEUE {
        @Override
        <T> RingBufferInterface<T> create(int capacity) {
            return MpMcRingBufferImpl.AGRONA_QUEUE.create(capacity);
        }
    };

    abstract <T> RingBufferInterface<T> create(int capacity);

    static class JCTSpScQueueAdapter<T> implements RingBufferInterface<T> {
        private final SpscArrayQueue<T> buffer;

        JCTSpScQueueAdapter(int capacity) {
            this.buffer = new SpscArrayQueue<>(capacity);
        }

        @Override
        public boolean offer(T item) {
            return buffer.offer(item);
        }

        @Override
        public T poll() {
            return buffer.poll();
        }

        @Override
        public int getSize() {
            return buffer.size();
        }

        @Override
        public void clear() {
            buffer.clear();
        }
    }

    static class AgronaSpScQueueAdapter<T> implements RingBufferInterface<T> {
        private final OneToOneConcurrentArrayQueue<T> buffer;

        AgronaSpScQueueAdapter(int capacity) {
            this.buffer = new OneToOneConcurrentArrayQueue<>(capacity);
        }

        @Override
        public boolean offer(T item) {
            return buffer.offer(item);
        }

        @Override
        public T poll() {
            return buffer.poll();
        }

        @Override
        public int getSize() {
            return buffer.size();
        }

        @Override
        public void clear() {
            buffer.clear();
        }
    }
}
