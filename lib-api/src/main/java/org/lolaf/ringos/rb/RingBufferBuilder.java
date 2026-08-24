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
package org.lolaf.ringos.rb;

import java.util.function.IntFunction;

/**
 * Builds {@link RingBuffer} instances for one memory-access implementation.
 *
 * <p>This is the seam behind {@link RingBufferFactory}: each ring-buffer implementation module ships a builder,
 * and the factory instantiates the one a {@link RingBufferBuilderProvider} names for the current runtime.
 * Applications call {@link RingBufferFactory} and never touch a builder directly.
 */
public interface RingBufferBuilder {

    /**
     * Builds a ring buffer of this builder's implementation.
     *
     * @param accessType              the producer/consumer concurrency the buffer must tolerate; it selects the
     *                                implementation class, and the caller is held to it
     * @param capacity                number of slots, which must be a power of two greater than one
     * @param bufferPaddingEnabled    whether to pad both ends of the backing arrays, keeping the buffer's own
     *                                elements off the cache lines of whatever the allocator placed next to them
     * @param elementInstanceProducer builds the element pre-filling each slot, called once per slot with its
     *                                index; {@code null} leaves the slots empty, for a buffer whose producers
     *                                publish references through {@link RingBuffer#offer(Object)} rather than
     *                                mutating pooled elements
     * @param <T>                     type of the elements held by the buffer
     * @return the ring buffer
     * @throws IllegalArgumentException if {@code capacity} is not a power of two
     */
    <T> RingBuffer<T> build(RingBufferFactory.AccessType accessType, int capacity, boolean bufferPaddingEnabled, IntFunction<T> elementInstanceProducer);
}
