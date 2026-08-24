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

import org.agrona.concurrent.ManyToManyConcurrentArrayQueue;
import org.jctools.queues.MpmcArrayQueue;
import org.lolaf.ringos.rb.methodhandle.MethodHandleMpMcRingBuffer;
import org.lolaf.ringos.rb.unsafe.UnsafeMpMcRingBuffer;

/**
 * The implementations measured under a multi-producer / multi-consumer topology, and the baseline the
 * other two topologies compare themselves against.
 */
public enum MpMcRingBufferImpl {
    METHOD_HANDLE_API {
        @Override
        <T> RingBufferInterface<T> create(int capacity) {
            return new RingBufferAdapter<>(new MethodHandleMpMcRingBuffer<>(capacity, false, null));
        }
    },
    METHOD_HANDLE_API_PADDED {
        @Override
        <T> RingBufferInterface<T> create(int capacity) {
            return new RingBufferAdapter<>(new MethodHandleMpMcRingBuffer<>(capacity, true, null));
        }
    },
    UNSAFE_API {
        @Override
        <T> RingBufferInterface<T> create(int capacity) {
            return new RingBufferAdapter<>(new UnsafeMpMcRingBuffer<>(capacity, false, null));
        }
    },
    UNSAFE_API_PADDED {
        @Override
        <T> RingBufferInterface<T> create(int capacity) {
            return new RingBufferAdapter<>(new UnsafeMpMcRingBuffer<>(capacity, true, null));
        }
    },
    JCT_QUEUE {
        @Override
        <T> RingBufferInterface<T> create(int capacity) {
            return new JCTMpMcQueueAdapter<>(capacity);
        }
    },
    AGRONA_QUEUE {
        @Override
        <T> RingBufferInterface<T> create(int capacity) {
            return new AgronaMpMcQueueAdapter<>(capacity);
        }
    };

    abstract <T> RingBufferInterface<T> create(int capacity);

    static class JCTMpMcQueueAdapter<T> implements RingBufferInterface<T> {
        private final MpmcArrayQueue<T> buffer;

        JCTMpMcQueueAdapter(int capacity) {
            this.buffer = new MpmcArrayQueue<>(capacity);
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

    static class AgronaMpMcQueueAdapter<T> implements RingBufferInterface<T> {
        private final ManyToManyConcurrentArrayQueue<T> buffer;

        AgronaMpMcQueueAdapter(int capacity) {
            this.buffer = new ManyToManyConcurrentArrayQueue<>(capacity);
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
