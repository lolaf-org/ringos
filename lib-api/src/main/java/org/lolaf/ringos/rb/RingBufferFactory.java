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
import java.util.List;
import java.util.ServiceLoader;
import java.util.function.IntFunction;
import java.util.function.Supplier;

/**
 * Entry point for creating {@link RingBuffer} instances — the only ring-buffer API an application needs.
 *
 * <p>Which implementation backs the buffers is decided once, when this class initialises: it walks the
 * {@link RingBufferBuilderProvider} services on the classpath, keeps the one claiming the current runtime with
 * the lowest {@link RingBufferBuilderProvider#priority()}, and reflectively instantiates the
 * {@link RingBufferBuilder} it names. Several providers may claim the same runtime — the {@code MethodHandle}
 * builder claims every one of them — so priority is what makes an {@code Unsafe} builder win wherever it
 * applies. That is what lets a single dependency on {@code ringos-lib-impl-all} run on JDK 11 through 25, with
 * or without {@code jdk.internal.misc} opened, and why initialisation fails loudly — with the candidates it saw
 * — when no provider matches at all.
 *
 * <p>Buffer padding defaults to the value of the
 * {@code org.lolaf.ringos.rb.RingBufferFactory.defaultBufferPaddingEnabled} system property, read at class
 * initialisation; the overloads taking a {@code bufferPaddingEnabled} flag decide per buffer instead.
 */
@Slf4j
@UtilityClass
public class RingBufferFactory {

    private static final boolean DEFAULT_PADDING_ENABLED = Boolean.getBoolean("org.lolaf.ringos.rb.RingBufferFactory.defaultBufferPaddingEnabled");
    private static final RingBufferBuilder RING_BUFFER_BUILDER;

    static {
        // Each provider is asked whether it can run on the current runtime (JDK version + whether
        // jdk.internal.misc is opened) and the best-priority claimant's builder is then loaded reflectively —
        // so, e.g., the Unsafe-based builders are never loaded when --add-opens is absent. See
        // RingBufferBuilderProvider and RingBufferBuilderProviderSelector.
        RingBufferBuilderProvider chosen =
                RingBufferBuilderProviderSelector.select(ServiceLoader.load(RingBufferBuilderProvider.class));
        RING_BUFFER_BUILDER = instantiate(chosen.implementationClassName());
        log.info("Loaded RingBufferBuilder SPI {}", RING_BUFFER_BUILDER.getClass().getName());
    }

    // Reflectively builds the builder the chosen provider named. Referenced only by string (never linked by a
    // provider), so a builder whose ring buffers touch Unsafe is loaded only after its provider confirmed the
    // runtime supports it.
    private static RingBufferBuilder instantiate(String className) {
        try {
            return (RingBufferBuilder) Class.forName(className).getDeclaredConstructor().newInstance();
        } catch (ExceptionInInitializerError | InvocationTargetException err) {
            throw asRuntimeException(err.getCause(), className);
        } catch (ReflectiveOperationException err) {
            throw new IllegalStateException("Unable to instantiate RingBufferBuilder implementation " + className, err);
        }
    }

    private static RuntimeException asRuntimeException(Throwable cause, String className) {
        if (cause instanceof RuntimeException) {
            return (RuntimeException) cause;
        }
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        return new IllegalStateException("Unable to instantiate RingBufferBuilder implementation " + className, cause);
    }

    /**
     * Builds a ring buffer with empty slots, whose producers publish references through
     * {@link RingBuffer#offer(Object)}.
     *
     * @param accessType the producer/consumer concurrency the buffer must tolerate; the caller is held to it
     * @param capacity   number of slots, which must be a power of two greater than one
     * @param <T>        type of the elements held by the buffer
     * @return the ring buffer
     * @throws IllegalArgumentException if {@code capacity} is not a power of two
     */
    public static <T> RingBuffer<T> build(AccessType accessType, int capacity) {
        return build(accessType, capacity, DEFAULT_PADDING_ENABLED, (IntFunction<T>) null);
    }

    /**
     * Builds a ring buffer whose slots are pre-filled with pooled elements, for producers that publish by
     * mutating them through an {@code EventTranslator…} overload.
     *
     * @param accessType              the producer/consumer concurrency the buffer must tolerate; the caller is
     *                                held to it
     * @param capacity                number of slots, which must be a power of two greater than one
     * @param elementInstanceProducer builds the element pre-filling each slot, called once per slot with its
     *                                index
     * @param <T>                     type of the elements held by the buffer
     * @return the ring buffer
     * @throws IllegalArgumentException if {@code capacity} is not a power of two
     */
    public static <T> RingBuffer<T> build(AccessType accessType, int capacity, IntFunction<T> elementInstanceProducer) {
        return build(accessType, capacity, DEFAULT_PADDING_ENABLED, elementInstanceProducer);
    }

    /**
     * Builds a ring buffer, deciding padding for this buffer rather than taking the system-property default.
     *
     * @param accessType              the producer/consumer concurrency the buffer must tolerate; the caller is
     *                                held to it
     * @param capacity                number of slots, which must be a power of two greater than one
     * @param bufferPaddingEnabled    whether to pad both ends of the backing arrays, keeping the buffer's own
     *                                elements off the cache lines of whatever the allocator placed next to them
     * @param elementInstanceProducer builds the element pre-filling each slot, called once per slot with its
     *                                index; {@code null} leaves the slots empty
     * @param <T>                     type of the elements held by the buffer
     * @return the ring buffer
     * @throws IllegalArgumentException if {@code capacity} is not a power of two
     */
    public static <T> RingBuffer<T> build(AccessType accessType, int capacity, boolean bufferPaddingEnabled, IntFunction<T> elementInstanceProducer) {
        return RING_BUFFER_BUILDER.build(accessType, capacity, bufferPaddingEnabled, elementInstanceProducer);
    }

    /**
     * Builds a ring buffer whose slots are pre-filled with pooled elements, for a supplier that does not care
     * which slot it is filling.
     *
     * @param accessType              the producer/consumer concurrency the buffer must tolerate; the caller is
     *                                held to it
     * @param capacity                number of slots, which must be a power of two greater than one
     * @param elementInstanceSupplier builds the element pre-filling each slot, called once per slot and expected
     *                                to return a fresh instance every time
     * @param <T>                     type of the elements held by the buffer
     * @return the ring buffer
     * @throws IllegalArgumentException if {@code capacity} is not a power of two
     */
    public static <T> RingBuffer<T> build(AccessType accessType, int capacity, Supplier<T> elementInstanceSupplier) {
        return build(accessType, capacity, DEFAULT_PADDING_ENABLED, elementInstanceSupplier);
    }

    /**
     * Builds a ring buffer pre-filled from a supplier, deciding padding for this buffer rather than taking the
     * system-property default.
     *
     * @param accessType              the producer/consumer concurrency the buffer must tolerate; the caller is
     *                                held to it
     * @param capacity                number of slots, which must be a power of two greater than one
     * @param bufferPaddingEnabled    whether to pad both ends of the backing arrays, keeping the buffer's own
     *                                elements off the cache lines of whatever the allocator placed next to them
     * @param elementInstanceSupplier builds the element pre-filling each slot, called once per slot and expected
     *                                to return a fresh instance every time
     * @param <T>                     type of the elements held by the buffer
     * @return the ring buffer
     * @throws IllegalArgumentException if {@code capacity} is not a power of two
     */
    public static <T> RingBuffer<T> build(AccessType accessType, int capacity, boolean bufferPaddingEnabled, Supplier<T> elementInstanceSupplier) {
        return RING_BUFFER_BUILDER.build(accessType, capacity, bufferPaddingEnabled, i -> elementInstanceSupplier.get());
    }

    /**
     * The producer/consumer concurrency a ring buffer must tolerate, and thereby the implementation it gets.
     * <p>
     * Pick the narrowest one that describes the actual callers: the cheaper variants skip the coordination the
     * wider ones pay for, so a buffer used more concurrently than it was declared corrupts silently rather than
     * failing.
     */
    public enum AccessType {
        /**
         * Single threaded poll(), single offer()
         */
        SINGLE_CONSUMER_SINGLE_PRODUCER,
        /**
         * Single threaded poll(), multithreaded offer()
         */
        SINGLE_CONSUMER_MULTI_PRODUCER,
        /**
         * Multithreaded poll(), single offer()
         */
        MULTI_CONSUMER_SINGLE_PRODUCER,
        /**
         * Multithreaded poll(), multithreaded offer()
         */
        MULTI_CONSUMER_MULTI_PRODUCER
    }

    /**
     * Thrown when the selected {@link RingBufferBuilder} needs JVM internals that were not opened to it, naming
     * the {@code --add-opens} arguments missing from the command line.
     */
    public class MissingAddOpensException extends RuntimeException {
        /**
         * @param addOpens the {@code module/package=target} arguments the builder needed, each rendered into the
         *                 message behind its own {@code --add-opens}
         * @param cause    the access failure that revealed them
         */
        public MissingAddOpensException(List<String> addOpens, Throwable cause) {
            super(generateErrorMessage(addOpens), cause);
        }

        private static String generateErrorMessage(List<String> addOpens) {
            StringBuilder msg = new StringBuilder("Missing JVM parameter ");
            addOpens.forEach(ao -> msg.append("--add-opens ").append(ao).append(" "));
            return msg.append("to unlock RingBufferBuilder API").toString();
        }
    }
}