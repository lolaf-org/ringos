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
package org.lolaf.ringos.rb.testkit;

import org.lolaf.ringos.idling.IdleStrategy;
import org.lolaf.ringos.rb.RingBuffer;
import org.lolaf.ringos.rb.RingBufferBuilder;
import org.lolaf.ringos.rb.RingBufferFactory.AccessType;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * What every test in this kit needs from the implementation under test, and the vocabulary the cases are
 * written in.
 *
 * <p>The single seam is {@link #builder()}: {@link RingBufferBuilder} already is the per-implementation factory
 * — one method, taking the access type, the capacity, the padding flag and the element instance producer — so a
 * concrete test is a class name and one {@code return new …RbBuilder();}. Building through it rather than
 * through the implementation classes also means the kit exercises the builder's own dispatch, and can never
 * assemble a combination the implementation does not offer.
 */
public abstract class AbstractRingBufferTestKit {

    /**
     * @return the builder of the implementation under test, e.g. {@code new UnsafeRbBuilder()}
     */
    protected abstract RingBufferBuilder builder();

    /**
     * Builds a buffer of the implementation under test in the shape {@code variant} describes.
     *
     * @param variant  which access type, and whether the slots are pre-filled and the array padded
     * @param capacity number of slots
     * @return the ring buffer
     */
    protected RingBuffer<Element> newBuffer(Variant variant, int capacity) {
        return newBuffer(variant, capacity, Element::new);
    }

    /**
     * Builds a buffer of the implementation under test over an element type of the caller's choosing.
     *
     * @param variant        which access type, and whether the slots are pre-filled and the array padded
     * @param capacity       number of slots
     * @param elementFactory builds the instance pre-filling each slot, ignored unless the variant is pooled
     * @param <T>            type of the elements held by the buffer
     * @return the ring buffer
     */
    protected <T> RingBuffer<T> newBuffer(Variant variant, int capacity, Supplier<T> elementFactory) {
        IntFunction<T> elementInstanceProducer = variant.isPooled() ? i -> elementFactory.get() : null;
        return builder().build(variant.getAccessType(), capacity, variant.isPadded(), elementInstanceProducer);
    }

    /**
     * Publishes one named element the way {@code variant} calls for: a pooled buffer is published into through a
     * translator, which is what its pre-filled instances exist for, and an unpooled one is handed a reference.
     *
     * @param buffer  the buffer to publish into
     * @param variant the shape it was built in
     * @param name    the name to give the element
     * @return {@code true} if it was published, {@code false} if the buffer was full
     */
    protected boolean publish(RingBuffer<Element> buffer, Variant variant, String name) {
        return variant.isPooled() ? buffer.offer(Element::setName, name) : buffer.offer(new Element(name));
    }

    /**
     * Publishes several named elements, asserting nothing: callers that care check the return of
     * {@link #publish} instead.
     *
     * @param buffer  the buffer to publish into
     * @param variant the shape it was built in
     * @param names   the names to give the elements, in order
     */
    protected void publishAll(RingBuffer<Element> buffer, Variant variant, String... names) {
        for (String name : names) {
            publish(buffer, variant, name);
        }
    }

    /**
     * The blocking counterpart of {@link #publish}, waiting on {@code idleStrategy} for room.
     *
     * @param buffer       the buffer to publish into
     * @param variant      the shape it was built in
     * @param name         the name to give the element
     * @param idleStrategy how to wait while the buffer is full
     */
    protected void publishBlocking(RingBuffer<Element> buffer, Variant variant, String name, IdleStrategy idleStrategy) {
        if (variant.isPooled()) {
            buffer.offerBlocking(Element::setName, name, idleStrategy);
        } else {
            buffer.offerBlocking(new Element(name), idleStrategy);
        }
    }

    /**
     * @param size     how many consumers the batch array should hold
     * @param consumer the consumer to put in every slot
     * @return an array for {@link RingBuffer#poll(Consumer[])}
     */
    @SuppressWarnings("unchecked")
    protected Consumer<Element>[] consumers(int size, Consumer<Element> consumer) {
        Consumer<Element>[] consumers = new Consumer[size];
        Arrays.fill(consumers, consumer);
        return consumers;
    }

    /**
     * One shape of buffer an implementation can be asked for: an access type, whether its slots hold pooled
     * instances, and whether its backing arrays are padded.
     *
     * <p>Padding is part of the matrix because it shifts every index by {@code BUFFER_PAD}, so an implementation
     * that computed one of them without it would be correct unpadded and wrong padded.
     */
    public static final class Variant {

        private final AccessType accessType;
        private final boolean pooled;
        private final boolean padded;

        private Variant(AccessType accessType, boolean pooled, boolean padded) {
            this.accessType = accessType;
            this.pooled = pooled;
            this.padded = padded;
        }

        /**
         * @return every shape an implementation must support: the four access types, pooled and not, padded and
         * not
         */
        public static Stream<Variant> all() {
            return Arrays.stream(AccessType.values())
                    .flatMap(accessType -> Stream.of(true, false)
                            .flatMap(pooled -> Stream.of(true, false)
                                    .map(padded -> new Variant(accessType, pooled, padded))));
        }

        /**
         * @param accessType the concurrency to build for
         * @return the four shapes of that access type: pooled and not, padded and not
         */
        public static Stream<Variant> of(AccessType accessType) {
            return all().filter(variant -> variant.getAccessType() == accessType);
        }

        /**
         * @return the producer/consumer concurrency the buffer is built for
         */
        public AccessType getAccessType() {
            return accessType;
        }

        /**
         * @return {@code true} if the slots are pre-filled with pooled element instances
         */
        public boolean isPooled() {
            return pooled;
        }

        /**
         * @return {@code true} if both ends of the backing arrays are padded
         */
        public boolean isPadded() {
            return padded;
        }

        @Override
        public String toString() {
            return accessType + (pooled ? " pooled" : " unpooled") + (padded ? " padded" : " unpadded");
        }
    }

    /**
     * The element the kit publishes: mutable, so a pooled slot can hold one instance and have a translator
     * populate it in place, and carrying enough fields for the widest translator overload.
     */
    public static final class Element {

        private final String[] args = new String[5];
        private String name;
        private long longArg;

        /**
         * Builds an empty element, as an element instance producer does for each slot of a pooled buffer.
         */
        public Element() {
        }

        /**
         * Builds a named element, as a producer publishing a reference into an unpooled buffer does.
         *
         * @param name the name to carry
         */
        public Element(String name) {
            this.name = name;
        }

        /**
         * @return the name this element carries
         */
        public String getName() {
            return name;
        }

        /**
         * @param name the name to carry; the one-argument translator target
         */
        public void setName(String name) {
            this.name = name;
        }

        /**
         * @return the five translator arguments, unset ones {@code null}
         */
        public String[] getArgs() {
            return args;
        }

        /**
         * @return the primitive argument of the long-carrying translator
         */
        public long getLongArg() {
            return longArg;
        }

        /**
         * @param values the translator arguments to record, in order
         */
        public void set(String... values) {
            Arrays.fill(args, null);
            System.arraycopy(values, 0, args, 0, values.length);
        }

        /**
         * @param longArg the primitive argument to record
         * @param values  the reference arguments to record, in order
         */
        public void set(long longArg, String... values) {
            this.longArg = longArg;
            set(values);
        }

        @Override
        public String toString() {
            return "Element(" + name + ")";
        }
    }
}
