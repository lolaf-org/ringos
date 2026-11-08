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

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;
import java.util.ServiceLoader;

/**
 * Entry point for creating byte ring buffers. The implementation is chosen once, on first use, the way
 * {@link RingBufferFactory} chooses its own.
 */
@UtilityClass
public class ByteRingBufferFactory {

    /**
     * The smallest capacity accepted, so the largest frame, an eighth of it, still carries a useful payload.
     */
    public static final int MIN_CAPACITY = 1024;

    /**
     * @param capacity in bytes, a power of two of at least {@link #MIN_CAPACITY}
     * @throws IllegalArgumentException for any other capacity
     */
    public static ByteRingBuffer queue(int capacity) {
        checkCapacity(capacity);
        return Builder.INSTANCE.buildQueue(capacity);
    }

    /**
     * @param capacity in bytes, a power of two of at least {@link #MIN_CAPACITY}
     * @throws IllegalArgumentException for any other capacity
     */
    public static BroadcastByteRingBuffer broadcast(int capacity, ProducerType producers) {
        checkCapacity(capacity);
        return Builder.INSTANCE.buildBroadcast(capacity, producers);
    }

    static int checkCapacity(int capacity) {
        if (capacity < MIN_CAPACITY || Integer.bitCount(capacity) != 1) {
            throw new IllegalArgumentException("Capacity must be a power of two of at least " + MIN_CAPACITY + " bytes, got " + capacity);
        }
        return capacity;
    }

    /**
     * Resolved apart from the factory, so an invalid capacity is refused even with no implementation on the
     * classpath.
     */
    @Slf4j
    private static final class Builder {

        private static final ByteRingBufferBuilder INSTANCE = instantiate(RingBufferBuilderProviderSelector.select(
                ServiceLoader.load(ByteRingBufferBuilderProvider.class), ByteRingBufferBuilderProvider.class)
                .implementationClassName());

        private static ByteRingBufferBuilder instantiate(String className) {
            try {
                ByteRingBufferBuilder builder = (ByteRingBufferBuilder) Class.forName(className).getDeclaredConstructor().newInstance();
                log.info("Loaded ByteRingBufferBuilder SPI {}", className);
                return builder;
            } catch (InvocationTargetException e) {
                throw new IllegalStateException("Unable to instantiate ByteRingBufferBuilder implementation " + className, e.getCause());
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("Unable to instantiate ByteRingBufferBuilder implementation " + className, e);
            }
        }
    }
}
