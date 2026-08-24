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


import org.lolaf.ringos.idling.IdleStrategy;

import java.time.Duration;
import java.util.function.Consumer;

/**
 * A pre-allocated, fixed-capacity ring buffer, the core queue of ringos.
 *
 * <p>Instances come from {@link RingBufferFactory#build}, which picks an implementation matching the requested
 * {@link RingBufferFactory.AccessType}: a single-producer or single-consumer buffer is cheaper than a fully
 * concurrent one, but only tolerates the concurrency it was built for. Polling from two threads on a
 * single-consumer buffer, or offering from two threads on a single-producer one, corrupts it silently — the
 * access type is a contract, not a hint.
 *
 * <p>Capacity is a power of two fixed at construction and the buffer never grows: {@link #offer(Object)} returns
 * {@code false} when full and {@link #poll()} returns {@code null} when empty, rather than blocking or
 * allocating. The {@code offerBlocking}/{@code pollBlocking} variants turn that refusal into a wait, idling on an
 * {@link IdleStrategy} until the operation succeeds.
 *
 * <p><b>Element reuse.</b> A buffer built with an element instance producer pre-fills every slot with a long-lived
 * element that is mutated in place rather than replaced, so publishing allocates nothing. Producers then publish
 * through one of the {@code EventTranslator…} overloads, which hand them the slot's own element to populate;
 * {@link #offer(Object)} instead stores the reference it is given, and suits a buffer built without a producer.
 * An element handed to a consumer stays owned by the buffer: it is only valid until that consumer polls again,
 * after which a producer may overwrite it, so anything kept beyond the callback must be copied out.
 *
 * <p>Size, emptiness and head/tail accessors are point-in-time estimates as soon as another thread is running.
 * They are meant for monitoring and sizing decisions, not for guarding a following {@code offer} or {@code poll} —
 * only the return value of the operation itself says whether it happened.
 *
 * @param <T> type of the elements held by the buffer
 */
public interface RingBuffer<T> {
    /**
     * Removes the element at the head of the buffer and returns it.
     *
     * @return the element removed, or {@code null} if the buffer is empty
     */
    T poll();

    /**
     * Removes the element at the head of the buffer, waiting on {@code idleStrategy} for as long as the buffer
     * stays empty.
     *
     * @param idleStrategy how to wait between polls; {@link IdleStrategy#reset()} is called on it before the
     *                     first idle, and not at all when an element is available straight away
     * @return the element removed, never {@code null}
     */
    T pollBlocking(IdleStrategy idleStrategy);

    /**
     * Removes the element at the head of the buffer, waiting on {@code idleStrategy} until one becomes available
     * or {@code maxWaitTime} elapses.
     *
     * @param idleStrategy how to wait between polls; {@link IdleStrategy#reset()} is called on it before the
     *                     first idle
     * @param maxWaitTime  how long to keep waiting; checked between idles, so it bounds the wait only as tightly
     *                     as the strategy's idle period allows
     * @return the element removed, or {@code null} if the buffer stayed empty for {@code maxWaitTime}
     */
    T pollBlocking(IdleStrategy idleStrategy, Duration maxWaitTime);

    /**
     * Removes the element at the head of the buffer and passes it to {@code consumer}.
     * <p>
     * The slot is only released once the consumer returns, so a slow consumer holds the buffer's head back rather
     * than exposing the element to a producer.
     *
     * @param consumer receives the element removed; it must not retain it beyond the call, as the buffer may
     *                 hand the same instance back to a producer afterwards
     * @return {@code true} if an element was consumed, {@code false} if the buffer was empty
     */
    boolean poll(Consumer<T> consumer);

    /**
     * Consumes elements until the buffer is empty.
     * <p>
     * Emptiness is judged one poll at a time, so against a producer that keeps up this drains for as long as the
     * producer publishes rather than returning at a snapshot of the queue.
     *
     * @param consumer receives each element removed, under the retention rule of {@link #poll(Consumer)}
     */
    default void drain(Consumer<T> consumer) {
        while (poll(consumer)) {
            // drain
        }
    }

    /**
     * Consumes exactly one element, waiting on {@code idleStrategy} for as long as the buffer stays empty.
     *
     * @param consumer     receives the element removed, under the retention rule of {@link #poll(Consumer)}
     * @param idleStrategy how to wait between polls; {@link IdleStrategy#reset()} is called on it before the
     *                     first idle, and not at all when an element is available straight away
     */
    void pollBlocking(Consumer<T> consumer, IdleStrategy idleStrategy);

    /**
     * Consumes a batch of up to {@code consumers.length} elements in one pass, handing the n-th element removed to
     * {@code consumers[n]}. Claiming the batch costs a single head update, which is what makes this cheaper than
     * the equivalent run of {@link #poll(Consumer)} calls.
     *
     * @param consumers one consumer per batch slot, each bound by the retention rule of {@link #poll(Consumer)};
     *                  its length caps the batch size
     * @return how many elements were consumed, {@code 0} if the buffer was empty
     */
    int poll(Consumer<T>[] consumers);

    /**
     * Returns the element at the head of the buffer without removing it.
     * <p>
     * On a buffer with more than one consumer the element may already be gone by the time this returns.
     *
     * @return the element at the head, or {@code null} if the buffer is empty
     */
    T peek();

    /**
     * Stores {@code element} at the tail of the buffer, waiting on {@code idleStrategy} for as long as the buffer
     * stays full.
     *
     * @param element      the element to store, under the ownership rule of {@link #offer(Object)}
     * @param idleStrategy how to wait between attempts; {@link IdleStrategy#reset()} is called on it before the
     *                     first idle, and not at all when there is room straight away
     */
    void offerBlocking(T element, IdleStrategy idleStrategy);

    /**
     * Stores {@code element} at the tail of the buffer, replacing whatever instance the slot held.
     * <p>
     * This is the overload for a buffer built without an element instance producer. On a pre-filled buffer it
     * throws away the pooled instance the slot was holding, so publish through an {@code EventTranslator…}
     * overload instead. The caller must not mutate the element afterwards: it is visible to a consumer as soon as
     * this returns.
     *
     * @param element the element to store, never {@code null}
     * @return {@code true} if it was stored, {@code false} if the buffer was full
     * @throws NullPointerException if {@code element} is {@code null} — a ring buffer holds no nulls, so that a
     *                              slot's own emptiness can stand for the absence of an element
     */
    boolean offer(T element);

    /**
     * Publishes at the tail by populating the slot's own element from {@code arg1}, waiting on
     * {@code idleStrategy} for as long as the buffer stays full.
     *
     * @param eventTranslator populates the slot's element; see {@link #offer(EventTranslatorOneArg, Object)}
     * @param arg1            passed through to the translator
     * @param idleStrategy    how to wait between attempts; {@link IdleStrategy#reset()} is called on it before
     *                        the first idle
     * @param <A>             type of the translator argument
     */
    <A> void offerBlocking(EventTranslatorOneArg<T, A> eventTranslator, A arg1, IdleStrategy idleStrategy);

    /**
     * Publishes at the tail by populating the slot's own element from {@code arg1}, allocating nothing.
     * <p>
     * The translator runs while the slot is still private to this producer; the element becomes visible to
     * consumers once it returns. It is only called when there is room, so a full buffer costs nothing but the
     * failed claim.
     *
     * @param eventTranslator populates the slot's element from the argument
     * @param arg1            passed through to the translator
     * @param <A>             type of the translator argument
     * @return {@code true} if the element was published, {@code false} if the buffer was full
     */
    <A> boolean offer(EventTranslatorOneArg<T, A> eventTranslator, A arg1);

    /**
     * Publishes at the tail by populating the slot's own element from two arguments, waiting on
     * {@code idleStrategy} for as long as the buffer stays full.
     *
     * @param eventTranslator populates the slot's element; see {@link #offer(EventTranslatorTwoArg, Object, Object)}
     * @param arg1            passed through to the translator
     * @param arg2            passed through to the translator
     * @param idleStrategy    how to wait between attempts; {@link IdleStrategy#reset()} is called on it before
     *                        the first idle
     * @param <A>             type of the first translator argument
     * @param <B>             type of the second translator argument
     */
    <A, B> void offerBlocking(EventTranslatorTwoArg<T, A, B> eventTranslator, A arg1, B arg2, IdleStrategy idleStrategy);

    /**
     * Publishes at the tail by populating the slot's own element from two arguments, allocating nothing.
     *
     * @param eventTranslator populates the slot's element from the arguments, under the visibility rule of
     *                        {@link #offer(EventTranslatorOneArg, Object)}
     * @param arg1            passed through to the translator
     * @param arg2            passed through to the translator
     * @param <A>             type of the first translator argument
     * @param <B>             type of the second translator argument
     * @return {@code true} if the element was published, {@code false} if the buffer was full
     */
    <A, B> boolean offer(EventTranslatorTwoArg<T, A, B> eventTranslator, A arg1, B arg2);

    /**
     * Publishes at the tail by populating the slot's own element from three arguments, waiting on
     * {@code idleStrategy} for as long as the buffer stays full.
     *
     * @param eventTranslator populates the slot's element; see
     *                        {@link #offer(EventTranslatorThreeArg, Object, Object, Object)}
     * @param arg1            passed through to the translator
     * @param arg2            passed through to the translator
     * @param arg3            passed through to the translator
     * @param idleStrategy    how to wait between attempts; {@link IdleStrategy#reset()} is called on it before
     *                        the first idle
     * @param <A>             type of the first translator argument
     * @param <B>             type of the second translator argument
     * @param <C>             type of the third translator argument
     */
    <A, B, C> void offerBlocking(EventTranslatorThreeArg<T, A, B, C> eventTranslator, A arg1, B arg2, C arg3, IdleStrategy idleStrategy);

    /**
     * Publishes at the tail by populating the slot's own element from a {@code long} and two references, waiting
     * on {@code idleStrategy} for as long as the buffer stays full.
     *
     * @param eventTranslator populates the slot's element; see
     *                        {@link #offer(EventTranslatorThreeLongArg, long, Object, Object)}
     * @param arg1            passed through to the translator, unboxed
     * @param arg2            passed through to the translator
     * @param arg3            passed through to the translator
     * @param idleStrategy    how to wait between attempts; {@link IdleStrategy#reset()} is called on it before
     *                        the first idle
     * @param <A>             type of the second translator argument
     * @param <B>             type of the third translator argument
     */
    <A, B> void offerBlocking(EventTranslatorThreeLongArg<T, A, B> eventTranslator, long arg1, A arg2, B arg3, IdleStrategy idleStrategy);

    /**
     * Publishes at the tail by populating the slot's own element from a {@code long} and two references,
     * allocating nothing.
     * <p>
     * The primitive first argument is what separates this from
     * {@link #offer(EventTranslatorThreeArg, Object, Object, Object)}: a timestamp or sequence number travels to
     * the translator without being boxed.
     *
     * @param eventTranslator populates the slot's element from the arguments, under the visibility rule of
     *                        {@link #offer(EventTranslatorOneArg, Object)}
     * @param arg1            passed through to the translator, unboxed
     * @param arg2            passed through to the translator
     * @param arg3            passed through to the translator
     * @param <A>             type of the second translator argument
     * @param <B>             type of the third translator argument
     * @return {@code true} if the element was published, {@code false} if the buffer was full
     */
    <A, B> boolean offer(EventTranslatorThreeLongArg<T, A, B> eventTranslator, long arg1, A arg2, B arg3);

    /**
     * Publishes at the tail by populating the slot's own element from three arguments, allocating nothing.
     *
     * @param eventTranslator populates the slot's element from the arguments, under the visibility rule of
     *                        {@link #offer(EventTranslatorOneArg, Object)}
     * @param arg1            passed through to the translator
     * @param arg2            passed through to the translator
     * @param arg3            passed through to the translator
     * @param <A>             type of the first translator argument
     * @param <B>             type of the second translator argument
     * @param <C>             type of the third translator argument
     * @return {@code true} if the element was published, {@code false} if the buffer was full
     */
    <A, B, C> boolean offer(EventTranslatorThreeArg<T, A, B, C> eventTranslator, A arg1, B arg2, C arg3);

    /**
     * Publishes at the tail by populating the slot's own element from four arguments, waiting on
     * {@code idleStrategy} for as long as the buffer stays full.
     *
     * @param eventTranslator populates the slot's element; see
     *                        {@link #offer(EventTranslatorFourArg, Object, Object, Object, Object)}
     * @param arg1            passed through to the translator
     * @param arg2            passed through to the translator
     * @param arg3            passed through to the translator
     * @param arg4            passed through to the translator
     * @param idleStrategy    how to wait between attempts; {@link IdleStrategy#reset()} is called on it before
     *                        the first idle
     * @param <A>             type of the first translator argument
     * @param <B>             type of the second translator argument
     * @param <C>             type of the third translator argument
     * @param <D>             type of the fourth translator argument
     */
    <A, B, C, D> void offerBlocking(EventTranslatorFourArg<T, A, B, C, D> eventTranslator, A arg1, B arg2, C arg3, D arg4, IdleStrategy idleStrategy);

    /**
     * Publishes at the tail by populating the slot's own element from four arguments, allocating nothing.
     *
     * @param eventTranslator populates the slot's element from the arguments, under the visibility rule of
     *                        {@link #offer(EventTranslatorOneArg, Object)}
     * @param arg1            passed through to the translator
     * @param arg2            passed through to the translator
     * @param arg3            passed through to the translator
     * @param arg4            passed through to the translator
     * @param <A>             type of the first translator argument
     * @param <B>             type of the second translator argument
     * @param <C>             type of the third translator argument
     * @param <D>             type of the fourth translator argument
     * @return {@code true} if the element was published, {@code false} if the buffer was full
     */
    <A, B, C, D> boolean offer(EventTranslatorFourArg<T, A, B, C, D> eventTranslator, A arg1, B arg2, C arg3, D arg4);

    /**
     * Publishes at the tail by populating the slot's own element from five arguments, waiting on
     * {@code idleStrategy} for as long as the buffer stays full.
     *
     * @param eventTranslator populates the slot's element; see
     *                        {@link #offer(EventTranslatorFiveArg, Object, Object, Object, Object, Object)}
     * @param arg1            passed through to the translator
     * @param arg2            passed through to the translator
     * @param arg3            passed through to the translator
     * @param arg4            passed through to the translator
     * @param arg5            passed through to the translator
     * @param idleStrategy    how to wait between attempts; {@link IdleStrategy#reset()} is called on it before
     *                        the first idle
     * @param <A>             type of the first translator argument
     * @param <B>             type of the second translator argument
     * @param <C>             type of the third translator argument
     * @param <D>             type of the fourth translator argument
     * @param <E>             type of the fifth translator argument
     */
    <A, B, C, D, E> void offerBlocking(EventTranslatorFiveArg<T, A, B, C, D, E> eventTranslator, A arg1, B arg2, C arg3, D arg4, E arg5, IdleStrategy idleStrategy);

    /**
     * Publishes at the tail by populating the slot's own element from five arguments, allocating nothing.
     *
     * @param eventTranslator populates the slot's element from the arguments, under the visibility rule of
     *                        {@link #offer(EventTranslatorOneArg, Object)}
     * @param arg1            passed through to the translator
     * @param arg2            passed through to the translator
     * @param arg3            passed through to the translator
     * @param arg4            passed through to the translator
     * @param arg5            passed through to the translator
     * @param <A>             type of the first translator argument
     * @param <B>             type of the second translator argument
     * @param <C>             type of the third translator argument
     * @param <D>             type of the fourth translator argument
     * @param <E>             type of the fifth translator argument
     * @return {@code true} if the element was published, {@code false} if the buffer was full
     */
    <A, B, C, D, E> boolean offer(EventTranslatorFiveArg<T, A, B, C, D, E> eventTranslator, A arg1, B arg2, C arg3, D arg4, E arg5);

    /**
     * Walks the elements currently published, from head to tail, without consuming any of them.
     * <p>
     * Head and tail are read once at entry and the walk skips slots that were consumed or republished meanwhile,
     * so with producers running this is a best-effort view rather than a snapshot. Intended for inspection and
     * monitoring, not for draining — use {@link #drain(Consumer)} for that.
     *
     * @param consumer receives each published element, under the retention rule of {@link #poll(Consumer)}
     */
    void forEach(Consumer<T> consumer);

    /**
     * Walks every element instance held by the backing array, published or not.
     * <p>
     * On a buffer pre-filled by an element instance producer this visits the whole pool, which is what makes it
     * useful for one-off work over the pooled instances — warming them up, sizing them, releasing what they hold.
     * It ignores head and tail entirely, so it also hands out elements a consumer has already taken.
     *
     * @param consumer receives each element instance in the backing array
     */
    void forEachEntry(Consumer<T> consumer);

    /**
     * Discards everything published and returns the buffer to its initial state, keeping the pooled element
     * instances in place.
     * <p>
     * It resets head, tail and the slot sequences without coordinating with anybody, so it is only safe while no
     * producer or consumer is touching the buffer.
     */
    void clear();

    /**
     * @return how many elements are currently published, i.e. the distance between head and tail. An estimate
     * while other threads are running, since the two ends are read separately
     */
    int getSize();

    /**
     * @return {@code true} if the buffer held no published element at the moment it was checked; a producer may
     * have published one by the time this returns
     */
    boolean isEmpty();

    /**
     * @return the negation of {@link #isEmpty()}, with the same caveat
     */
    boolean isNotEmpty();

    /**
     * @return {@code true} if the buffer held {@link #getCapacity()} published elements at the moment it was
     * checked; a consumer may have freed a slot by the time this returns
     */
    boolean isFull();

    /**
     * @return the number of slots in the buffer, a power of two fixed at construction
     */
    int getCapacity();

    /**
     * @return the head position: the running count of elements consumed since the buffer was created or last
     * {@link #clear() cleared}, not an index into the backing array
     */
    long getCurrentHead();

    /**
     * @return the tail position: the running count of elements published since the buffer was created or last
     * {@link #clear() cleared}, not an index into the backing array
     */
    long getCurrentTail();

    /**
     * Populates a buffer slot's element from one argument.
     *
     * @param <T> type of the elements held by the buffer
     * @param <A> type of the argument
     */
    @FunctionalInterface
    interface EventTranslatorOneArg<T, A> {
        /**
         * Writes {@code arg1} into {@code event}, which is the buffer's own instance for the claimed slot and
         * must be mutated in place rather than replaced.
         *
         * @param event the slot's element, still private to the publishing producer
         * @param arg1  the value to write
         */
        void translate(T event, A arg1);
    }

    /**
     * Populates a buffer slot's element from two arguments.
     *
     * @param <T> type of the elements held by the buffer
     * @param <A> type of the first argument
     * @param <B> type of the second argument
     */
    @FunctionalInterface
    interface EventTranslatorTwoArg<T, A, B> {
        /**
         * Writes the arguments into {@code event}, under the in-place rule of
         * {@link EventTranslatorOneArg#translate}.
         *
         * @param event the slot's element, still private to the publishing producer
         * @param arg1  the first value to write
         * @param arg2  the second value to write
         */
        void translate(T event, A arg1, B arg2);
    }

    /**
     * Populates a buffer slot's element from three arguments.
     *
     * @param <T> type of the elements held by the buffer
     * @param <A> type of the first argument
     * @param <B> type of the second argument
     * @param <C> type of the third argument
     */
    @FunctionalInterface
    interface EventTranslatorThreeArg<T, A, B, C> {
        /**
         * Writes the arguments into {@code event}, under the in-place rule of
         * {@link EventTranslatorOneArg#translate}.
         *
         * @param event the slot's element, still private to the publishing producer
         * @param arg1  the first value to write
         * @param arg2  the second value to write
         * @param arg3  the third value to write
         */
        void translate(T event, A arg1, B arg2, C arg3);
    }

    /**
     * Populates a buffer slot's element from a {@code long} and two references, so that a primitive first
     * argument — a timestamp, a sequence number — reaches the element without being boxed.
     *
     * @param <T> type of the elements held by the buffer
     * @param <A> type of the second argument
     * @param <B> type of the third argument
     */
    @FunctionalInterface
    interface EventTranslatorThreeLongArg<T, A, B> {
        /**
         * Writes the arguments into {@code event}, under the in-place rule of
         * {@link EventTranslatorOneArg#translate}.
         *
         * @param event the slot's element, still private to the publishing producer
         * @param arg1  the primitive value to write
         * @param arg2  the second value to write
         * @param arg3  the third value to write
         */
        void translate(T event, long arg1, A arg2, B arg3);
    }

    /**
     * Populates a buffer slot's element from four arguments.
     *
     * @param <T> type of the elements held by the buffer
     * @param <A> type of the first argument
     * @param <B> type of the second argument
     * @param <C> type of the third argument
     * @param <D> type of the fourth argument
     */
    @FunctionalInterface
    interface EventTranslatorFourArg<T, A, B, C, D> {
        /**
         * Writes the arguments into {@code event}, under the in-place rule of
         * {@link EventTranslatorOneArg#translate}.
         *
         * @param event the slot's element, still private to the publishing producer
         * @param arg1  the first value to write
         * @param arg2  the second value to write
         * @param arg3  the third value to write
         * @param arg4  the fourth value to write
         */
        void translate(T event, A arg1, B arg2, C arg3, D arg4);
    }

    /**
     * Populates a buffer slot's element from five arguments.
     *
     * @param <T> type of the elements held by the buffer
     * @param <A> type of the first argument
     * @param <B> type of the second argument
     * @param <C> type of the third argument
     * @param <D> type of the fourth argument
     * @param <E> type of the fifth argument
     */
    @FunctionalInterface
    interface EventTranslatorFiveArg<T, A, B, C, D, E> {
        /**
         * Writes the arguments into {@code event}, under the in-place rule of
         * {@link EventTranslatorOneArg#translate}.
         *
         * @param event the slot's element, still private to the publishing producer
         * @param arg1  the first value to write
         * @param arg2  the second value to write
         * @param arg3  the third value to write
         * @param arg4  the fourth value to write
         * @param arg5  the fifth value to write
         */
        void translate(T event, A arg1, B arg2, C arg3, D arg4, E arg5);
    }
}
