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
package org.lolaf.ringos.rb.unsafe11;

import org.lolaf.ringos.rb.RingBuffer;
import org.lolaf.ringos.rb.RingBufferBuilder;
import org.lolaf.ringos.rb.RingBufferFactory;

import java.util.List;
import java.util.function.IntFunction;

public class J11UnsafeRbBuilder implements RingBufferBuilder {

    @Override
    public <T> RingBuffer<T> build(RingBufferFactory.AccessType accessType, int capacity, boolean bufferPaddingEnabled, IntFunction<T> elementInstanceProducer) {
        try {
            switch (accessType) {
                case SINGLE_CONSUMER_SINGLE_PRODUCER:
                    return new J11UnsafeSpScRingBuffer<>(capacity, bufferPaddingEnabled, elementInstanceProducer);
                case SINGLE_CONSUMER_MULTI_PRODUCER:
                    return elementInstanceProducer == null
                            ? new J11UnsafeMpScRingBuffer<>(capacity, bufferPaddingEnabled)
                            : new J11UnsafePooledMpScRingBuffer<>(capacity, bufferPaddingEnabled, elementInstanceProducer);
                case MULTI_CONSUMER_SINGLE_PRODUCER:
                    return new J11UnsafeSpMcRingBuffer<>(capacity, bufferPaddingEnabled, elementInstanceProducer);
                case MULTI_CONSUMER_MULTI_PRODUCER:
                    return new J11UnsafeMpMcRingBuffer<>(capacity, bufferPaddingEnabled, elementInstanceProducer);
                default:
                    throw new IllegalStateException("not implemented " + accessType);
            }
        } catch (IllegalAccessError ex) {
            throw new RingBufferFactory.MissingAddOpensException(List.of("java.base/jdk.internal.misc=ALL-UNNAMED"), ex);
        }
    }
}