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

import org.lolaf.ringos.rb.RingBuffer;

/**
 * Drives a ringos {@link RingBuffer} through {@link RingBufferInterface}.
 *
 * @param <T> the element type
 */
final class RingBufferAdapter<T> implements RingBufferInterface<T> {

    private final RingBuffer<T> buffer;

    RingBufferAdapter(RingBuffer<T> ringbuffer) {
        this.buffer = ringbuffer;
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
        return buffer.getSize();
    }

    @Override
    public void clear() {
        buffer.clear();
    }
}
