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

import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.jctools.queues.MpscArrayQueue;
import org.lolaf.ringos.rb.methodhandle.MethodHandleMpScRingBuffer;
import org.lolaf.ringos.rb.methodhandle.MethodHandlePooledMpScRingBuffer;
import org.lolaf.ringos.rb.unsafe.UnsafeMpScRingBuffer;
import org.lolaf.ringos.rb.unsafe.UnsafePooledMpScRingBuffer;

/**
 * The implementations measured under a multi-producer / single-consumer topology.
 *
 * <p>The single-consumer ring buffers are the subject of the comparison; the many-to-many entries
 * delegate to {@link MpMcRingBufferImpl} and are measured as a baseline, so the cost of running a
 * multi-consumer queue on a single-consumer topology can be read off directly. Single-producer
 * implementations are deliberately absent: two producer threads would corrupt them.
 */
public enum MpScRingBufferImpl {
    METHOD_HANDLE_API_MPSC {
        @Override
        <T> RingBufferInterface<T> create(int capacity) {
            return new RingBufferAdapter<>(new MethodHandleMpScRingBuffer<>(capacity, false));
        }
    },
    METHOD_HANDLE_API_MPSC_PADDED {
        @Override
        <T> RingBufferInterface<T> create(int capacity) {
            return new RingBufferAdapter<>(new MethodHandleMpScRingBuffer<>(capacity, true));
        }
    },
    UNSAFE_API_MPSC {
        @Override
        <T> RingBufferInterface<T> create(int capacity) {
            return new RingBufferAdapter<>(new UnsafeMpScRingBuffer<>(capacity, false));
        }
    },
    UNSAFE_API_MPSC_PADDED {
        @Override
        <T> RingBufferInterface<T> create(int capacity) {
            return new RingBufferAdapter<>(new UnsafeMpScRingBuffer<>(capacity, true));
        }
    },
    METHOD_HANDLE_API_MPSC_SEQUENCED {
        @Override
        <T> RingBufferInterface<T> create(int capacity) {
            return new RingBufferAdapter<>(new MethodHandlePooledMpScRingBuffer<>(capacity, false, null));
        }
    },
    UNSAFE_API_MPSC_SEQUENCED {
        @Override
        <T> RingBufferInterface<T> create(int capacity) {
            return new RingBufferAdapter<>(new UnsafePooledMpScRingBuffer<>(capacity, false, null));
        }
    },
    JCT_MPSC_QUEUE {
        @Override
        <T> RingBufferInterface<T> create(int capacity) {
            return new JCTMpScQueueAdapter<>(capacity);
        }
    },
    AGRONA_MPSC_QUEUE {
        @Override
        <T> RingBufferInterface<T> create(int capacity) {
            return new AgronaMpScQueueAdapter<>(capacity);
        }
    },
    METHOD_HANDLE_API_MPMC {
        @Override
        <T> RingBufferInterface<T> create(int capacity) {
            return MpMcRingBufferImpl.METHOD_HANDLE_API.create(capacity);
        }
    },
    METHOD_HANDLE_API_MPMC_PADDED {
        @Override
        <T> RingBufferInterface<T> create(int capacity) {
            return MpMcRingBufferImpl.METHOD_HANDLE_API_PADDED.create(capacity);
        }
    },
    UNSAFE_API_MPMC {
        @Override
        <T> RingBufferInterface<T> create(int capacity) {
            return MpMcRingBufferImpl.UNSAFE_API.create(capacity);
        }
    },
    UNSAFE_API_MPMC_PADDED {
        @Override
        <T> RingBufferInterface<T> create(int capacity) {
            return MpMcRingBufferImpl.UNSAFE_API_PADDED.create(capacity);
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

    static class JCTMpScQueueAdapter<T> implements RingBufferInterface<T> {
        private final MpscArrayQueue<T> buffer;

        JCTMpScQueueAdapter(int capacity) {
            this.buffer = new MpscArrayQueue<>(capacity);
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

    static class AgronaMpScQueueAdapter<T> implements RingBufferInterface<T> {
        private final ManyToOneConcurrentArrayQueue<T> buffer;

        AgronaMpScQueueAdapter(int capacity) {
            this.buffer = new ManyToOneConcurrentArrayQueue<>(capacity);
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
