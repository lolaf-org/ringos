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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.lolaf.ringos.idling.BackoffIdleStrategy;
import org.lolaf.ringos.idling.IdleStrategy;
import org.lolaf.ringos.rb.RingBuffer;
import org.lolaf.ringos.rb.RingBufferFactory.AccessType;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

/**
 * The single-threaded contract every {@link RingBuffer} implementation must satisfy, whatever
 * {@link AccessType} it was built for and however it reaches memory.
 *
 * <p>Cases run against every variant an implementation can produce — the four access types, with and without
 * pooled element instances, with and without buffer padding — so a behaviour that only holds for, say, an
 * unpadded single-consumer buffer fails here rather than in production. Three narrower sources cover the places
 * the variants genuinely differ: {@link #pooledVariants()} for the {@code EventTranslator} overloads, which need
 * a pooled instance to populate; {@link #slotFlaggedVariants()} for the one buffer that refuses them; and
 * {@link #blockingVariants()} for the cases that have to wait out a real quarter of a second.
 *
 * @see AbstractRingBufferConcurrencyTest for what the implementations must do with several threads on them
 */
public abstract class AbstractRingBufferContractTest extends AbstractRingBufferTestKit {

    private static final int CAPACITY = 8;

    static Stream<Variant> allVariants() {
        return Variant.all();
    }

    static Stream<Variant> pooledVariants() {
        return Variant.all().filter(Variant::isPooled);
    }

    /**
     * {@link #blockingVariants()} narrowed to the pooled ones, for the blocking translator overloads.
     */
    static Stream<Variant> pooledUnpaddedVariants() {
        return blockingVariants().filter(Variant::isPooled);
    }

    /**
     * The blocking overloads are wrappers that retry the non-blocking ones while idling, shared by every
     * implementation and indifferent to how the backing array is laid out — so they run over half the matrix,
     * and pay their quarter-second wait once per access type rather than twice.
     */
    static Stream<Variant> blockingVariants() {
        return Variant.all().filter(v -> !v.isPadded());
    }

    /**
     * The variants that land on the slot-flagged multi-producer/single-consumer buffer: single consumer, many
     * producers, no pooled instances. It is the only implementation that reads a slot's own emptiness rather
     * than a sequence beside it, and so the only one that cannot host a translator.
     */
    static Stream<Variant> slotFlaggedVariants() {
        return Variant.all()
                .filter(v -> v.getAccessType() == AccessType.SINGLE_CONSUMER_MULTI_PRODUCER)
                .filter(v -> !v.isPooled());
    }

    private static void fail(String message) {
        throw new AssertionError(message);
    }

    @ParameterizedTest
    @MethodSource("allVariants")
    void capacityMustBeAPowerOfTwoGreaterThanOne(Variant variant) {
        assertThatThrownBy(() -> newBuffer(variant, 1020))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Capacity 1020 must be a power of 2");
        assertThatThrownBy(() -> newBuffer(variant, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Capacity 1 must be a power of 2");
        assertThatThrownBy(() -> newBuffer(variant, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Capacity 0 must be a power of 2");

        for (int capacity : new int[]{2, 32, 512}) {
            assertThat(newBuffer(variant, capacity).getCapacity()).isEqualTo(capacity);
        }
    }

    @ParameterizedTest
    @MethodSource("allVariants")
    void aFreshBufferIsEmpty(Variant variant) {
        RingBuffer<Element> buffer = newBuffer(variant, CAPACITY);

        assertThat(buffer.isEmpty()).isTrue();
        assertThat(buffer.isNotEmpty()).isFalse();
        assertThat(buffer.isFull()).isFalse();
        assertThat(buffer.getSize()).isZero();
        assertThat(buffer.getCurrentHead()).isZero();
        assertThat(buffer.getCurrentTail()).isZero();
        assertThat(buffer.poll()).isNull();
        assertThat(buffer.peek()).isNull();
        assertThat(buffer.poll(e -> fail("nothing to consume"))).isFalse();
        assertThat(buffer.poll(consumers(2, e -> fail("nothing to consume")))).isZero();

        List<Element> seen = new ArrayList<>();
        buffer.forEach(seen::add);
        assertThat(seen).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("allVariants")
    void offerRejectsNullWithoutChangingTheBuffer(Variant variant) {
        RingBuffer<Element> buffer = newBuffer(variant, CAPACITY);
        publish(buffer, variant, "kept");

        assertThrows(NullPointerException.class, () -> buffer.offer(null));

        assertThat(buffer.getSize()).isEqualTo(1);
        assertThat(buffer.poll().getName()).isEqualTo("kept");
    }

    @ParameterizedTest
    @MethodSource("allVariants")
    void anOfferedElementComesBackOutOfPoll(Variant variant) {
        RingBuffer<Element> buffer = newBuffer(variant, CAPACITY);

        assertThat(publish(buffer, variant, "one")).isTrue();
        assertThat(buffer.getSize()).isEqualTo(1);
        assertThat(buffer.isNotEmpty()).isTrue();
        assertThat(buffer.isEmpty()).isFalse();

        assertThat(buffer.poll().getName()).isEqualTo("one");
        assertThat(buffer.isEmpty()).isTrue();
        assertThat(buffer.getSize()).isZero();
    }

    @ParameterizedTest
    @MethodSource("allVariants")
    void elementsComeBackInTheOrderTheyWerePublished(Variant variant) {
        RingBuffer<Element> buffer = newBuffer(variant, CAPACITY);

        publishAll(buffer, variant, "a", "b", "c");

        assertThat(buffer.poll().getName()).isEqualTo("a");
        assertThat(buffer.poll().getName()).isEqualTo("b");
        assertThat(buffer.poll().getName()).isEqualTo("c");
        assertThat(buffer.poll()).isNull();
    }

    @ParameterizedTest
    @MethodSource("allVariants")
    void aFullBufferRefusesFurtherElements(Variant variant) {
        RingBuffer<Element> buffer = newBuffer(variant, CAPACITY);

        for (int i = 0; i < CAPACITY; i++) {
            assertThat(publish(buffer, variant, "e" + i)).isTrue();
        }

        assertThat(buffer.isFull()).isTrue();
        assertThat(buffer.getSize()).isEqualTo(CAPACITY);
        assertThat(publish(buffer, variant, "overflow")).isFalse();
        assertThat(buffer.getSize()).isEqualTo(CAPACITY);

        for (int i = 0; i < CAPACITY; i++) {
            assertThat(buffer.poll().getName()).isEqualTo("e" + i);
        }
        assertThat(buffer.isFull()).isFalse();
        assertThat(buffer.isEmpty()).isTrue();

        // and it takes elements again once drained
        assertThat(publish(buffer, variant, "after")).isTrue();
        assertThat(buffer.poll().getName()).isEqualTo("after");
    }

    /**
     * Filling and draining repeatedly is what walks the backing array past its end, so a stale cached position or
     * an index that forgot the padding shows up here and nowhere in a single lap.
     */
    @ParameterizedTest
    @MethodSource("allVariants")
    void survivesRepeatedWrapArounds(Variant variant) {
        RingBuffer<Element> buffer = newBuffer(variant, CAPACITY);

        for (int round = 0; round < 200; round++) {
            for (int i = 0; i < CAPACITY; i++) {
                assertThat(publish(buffer, variant, "r" + round + "e" + i)).isTrue();
            }
            assertThat(buffer.isFull()).isTrue();
            assertThat(publish(buffer, variant, "overflow")).isFalse();

            for (int i = 0; i < CAPACITY; i++) {
                assertThat(buffer.poll().getName()).isEqualTo("r" + round + "e" + i);
            }
            assertThat(buffer.isEmpty()).isTrue();
            assertThat(buffer.poll()).isNull();
        }
    }

    /**
     * The other wrap-around shape: a buffer that is never allowed to fill, so head and tail chase each other
     * around the array one slot apart.
     */
    @ParameterizedTest
    @MethodSource("allVariants")
    void staysCorrectWhenTheBufferIsNeverAllowedToFill(Variant variant) {
        RingBuffer<Element> buffer = newBuffer(variant, CAPACITY);

        for (int i = 0; i < 5_000; i++) {
            assertThat(publish(buffer, variant, "e" + i)).isTrue();
            assertThat(buffer.poll().getName()).isEqualTo("e" + i);
        }
        assertThat(buffer.isEmpty()).isTrue();
    }

    @ParameterizedTest
    @MethodSource("allVariants")
    void handlesPartialFillsOfVaryingDepth(Variant variant) {
        RingBuffer<Element> buffer = newBuffer(variant, CAPACITY);
        Random random = new Random(20240816L);

        for (int round = 0; round < 1_024; round++) {
            int depth = random.nextInt(CAPACITY + 1);
            assertThat(buffer.isEmpty()).isTrue();
            for (int i = 0; i < depth; i++) {
                assertThat(publish(buffer, variant, "e" + i)).isTrue();
            }
            assertThat(buffer.getSize()).isEqualTo(depth);
            for (int i = 0; i < depth; i++) {
                assertThat(buffer.poll().getName()).isEqualTo("e" + i);
            }
            assertThat(buffer.isEmpty()).isTrue();
        }
    }

    @ParameterizedTest
    @MethodSource("allVariants")
    void peekDoesNotConsume(Variant variant) {
        RingBuffer<Element> buffer = newBuffer(variant, CAPACITY);
        assertThat(buffer.peek()).isNull();

        publishAll(buffer, variant, "first", "second");

        assertThat(buffer.peek().getName()).isEqualTo("first");
        assertThat(buffer.peek().getName()).isEqualTo("first");
        assertThat(buffer.getSize()).isEqualTo(2);

        assertThat(buffer.poll().getName()).isEqualTo("first");
        assertThat(buffer.peek().getName()).isEqualTo("second");
        assertThat(buffer.getSize()).isEqualTo(1);

        assertThat(buffer.poll().getName()).isEqualTo("second");
        assertThat(buffer.peek()).isNull();
    }

    @ParameterizedTest
    @MethodSource("allVariants")
    void pollHandsTheElementToAConsumer(Variant variant) {
        RingBuffer<Element> buffer = newBuffer(variant, CAPACITY);
        List<String> seen = new ArrayList<>();

        assertThat(buffer.poll(e -> seen.add(e.getName()))).isFalse();
        assertThat(seen).isEmpty();

        publish(buffer, variant, "only");

        assertThat(buffer.poll(e -> seen.add(e.getName()))).isTrue();
        assertThat(seen).containsExactly("only");
        assertThat(buffer.getSize()).isZero();

        assertThat(buffer.poll(e -> seen.add(e.getName()))).isFalse();
        assertThat(seen).containsExactly("only");
    }

    @ParameterizedTest
    @MethodSource("allVariants")
    void pollsABatchCappedByTheConsumerArray(Variant variant) {
        RingBuffer<Element> buffer = newBuffer(variant, CAPACITY);
        List<String> seen = new ArrayList<>();
        Consumer<Element>[] consumers = consumers(3, e -> seen.add(e.getName()));

        assertThat(buffer.poll(consumers)).isZero();

        // fewer elements than consumers: the batch stops at the first empty slot
        publishAll(buffer, variant, "a", "b");
        assertThat(buffer.poll(consumers)).isEqualTo(2);
        assertThat(seen).containsExactly("a", "b");
        assertThat(buffer.isEmpty()).isTrue();
        assertThat(buffer.poll(consumers)).isZero();

        // exactly as many elements as consumers
        seen.clear();
        publishAll(buffer, variant, "c", "d", "e");
        assertThat(buffer.poll(consumers)).isEqualTo(3);
        assertThat(seen).containsExactly("c", "d", "e");
        assertThat(buffer.isEmpty()).isTrue();

        // more elements than consumers: two passes, and the leftovers stay in order
        seen.clear();
        publishAll(buffer, variant, "1", "2", "3", "4", "5");
        assertThat(buffer.poll(consumers)).isEqualTo(3);
        assertThat(buffer.isEmpty()).isFalse();
        assertThat(buffer.poll(consumers)).isEqualTo(2);
        assertThat(seen).containsExactly("1", "2", "3", "4", "5");
        assertThat(buffer.isEmpty()).isTrue();
    }

    @ParameterizedTest
    @MethodSource("allVariants")
    void batchPollHandsTheNthElementToTheNthConsumer(Variant variant) {
        RingBuffer<Element> buffer = newBuffer(variant, CAPACITY);
        publishAll(buffer, variant, "a", "b");

        List<String> first = new ArrayList<>();
        List<String> second = new ArrayList<>();
        List<String> third = new ArrayList<>();
        @SuppressWarnings("unchecked")
        Consumer<Element>[] consumers = new Consumer[]{
                (Consumer<Element>) e -> first.add(e.getName()),
                (Consumer<Element>) e -> second.add(e.getName()),
                (Consumer<Element>) e -> third.add(e.getName())};

        assertThat(buffer.poll(consumers)).isEqualTo(2);
        assertThat(first).containsExactly("a");
        assertThat(second).containsExactly("b");
        assertThat(third).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("allVariants")
    void drainEmptiesTheBuffer(Variant variant) {
        RingBuffer<Element> buffer = newBuffer(variant, CAPACITY);
        publishAll(buffer, variant, "a", "b", "c");

        List<String> seen = new ArrayList<>();
        buffer.drain(e -> seen.add(e.getName()));

        assertThat(seen).containsExactly("a", "b", "c");
        assertThat(buffer.isEmpty()).isTrue();

        buffer.drain(e -> fail("nothing left to drain"));
    }

    @ParameterizedTest
    @MethodSource("allVariants")
    void forEachWalksWhatIsPublishedWithoutConsumingIt(Variant variant) {
        RingBuffer<Element> buffer = newBuffer(variant, CAPACITY);
        publishAll(buffer, variant, "Foo", "Bar", "Baz");

        assertThat(names(buffer::forEach)).containsExactly("Foo", "Bar", "Baz");
        assertThat(buffer.getSize()).isEqualTo(3);

        buffer.poll();
        assertThat(names(buffer::forEach)).containsExactly("Bar", "Baz");

        buffer.drain(e -> {
        });
        assertThat(names(buffer::forEach)).isEmpty();
    }

    /**
     * {@code forEachEntry} ignores head and tail and walks the backing array itself, which is what makes it the
     * way to reach a pooled buffer's whole instance pool — including the slots nothing has been published into.
     */
    @ParameterizedTest
    @MethodSource("allVariants")
    void forEachEntryWalksTheBackingArray(Variant variant) {
        RingBuffer<Element> buffer = newBuffer(variant, CAPACITY);

        if (variant.isPooled()) {
            // every slot already holds its pooled instance, published or not
            assertThat(count(buffer::forEachEntry)).isEqualTo(CAPACITY);
            publishAll(buffer, variant, "a", "b");
            assertThat(count(buffer::forEachEntry)).isEqualTo(CAPACITY);
            buffer.drain(e -> {
            });
            assertThat(count(buffer::forEachEntry)).isEqualTo(CAPACITY);
        } else {
            // nothing is stored until a producer stores it, and a consumer empties the slot again
            assertThat(count(buffer::forEachEntry)).isZero();
            publishAll(buffer, variant, "a", "b");
            assertThat(names(buffer::forEachEntry)).containsExactlyInAnyOrder("a", "b");
            buffer.poll();
            assertThat(names(buffer::forEachEntry)).containsExactly("b");
        }
    }

    @ParameterizedTest
    @MethodSource("allVariants")
    void clearDiscardsEverythingPublishedAndLeavesTheBufferUsable(Variant variant) {
        RingBuffer<Element> buffer = newBuffer(variant, CAPACITY);
        for (int i = 0; i < CAPACITY; i++) {
            publish(buffer, variant, "e" + i);
        }

        buffer.clear();

        assertThat(buffer.isEmpty()).isTrue();
        assertThat(buffer.isFull()).isFalse();
        assertThat(buffer.getSize()).isZero();
        assertThat(buffer.getCurrentHead()).isZero();
        assertThat(buffer.getCurrentTail()).isZero();
        assertThat(buffer.poll()).isNull();
        assertThat(buffer.peek()).isNull();
        assertThat(names(buffer::forEach)).isEmpty();

        if (variant.isPooled()) {
            // the pool survives a clear; that is the point of pre-filling the slots
            assertThat(count(buffer::forEachEntry)).isEqualTo(CAPACITY);
        }

        assertThat(publish(buffer, variant, "after clear")).isTrue();
        assertThat(buffer.poll().getName()).isEqualTo("after clear");
    }

    /**
     * Head and tail are running counts of what was consumed and published, not indices, so they keep climbing
     * past the capacity and only {@link RingBuffer#clear()} sends them back to zero.
     */
    @ParameterizedTest
    @MethodSource("allVariants")
    void headAndTailAreRunningCountsThatSurviveWrapAround(Variant variant) {
        RingBuffer<Element> buffer = newBuffer(variant, CAPACITY);

        int published = 0;
        int consumed = 0;
        for (int round = 0; round < 20; round++) {
            for (int i = 0; i < CAPACITY; i++) {
                publish(buffer, variant, "e" + i);
                published++;
                assertThat(buffer.getCurrentTail()).isEqualTo(published);
            }
            for (int i = 0; i < CAPACITY; i++) {
                buffer.poll();
                consumed++;
                assertThat(buffer.getCurrentHead()).isEqualTo(consumed);
            }
        }

        assertThat(published).isGreaterThan(CAPACITY);
        assertThat(buffer.getCurrentTail()).isEqualTo(published);
        assertThat(buffer.getCurrentHead()).isEqualTo(consumed);
        assertThat(buffer.getSize()).isZero();

        buffer.clear();
        assertThat(buffer.getCurrentHead()).isZero();
        assertThat(buffer.getCurrentTail()).isZero();
    }

    @ParameterizedTest
    @MethodSource("blockingVariants")
    void pollBlockingWaitsForAnElement(Variant variant) {
        RingBuffer<Element> buffer = newBuffer(variant, CAPACITY);
        IdleStrategy idleStrategy = spy(BackoffIdleStrategy.class);

        publishAfter(buffer, variant, "late", 250);

        assertThat(buffer.pollBlocking(idleStrategy).getName()).isEqualTo("late");

        verify(idleStrategy).reset();
        verify(idleStrategy, atLeast(10)).idle();
    }

    @ParameterizedTest
    @MethodSource("blockingVariants")
    void pollBlockingDoesNotIdleWhenAnElementIsAlreadyThere(Variant variant) {
        RingBuffer<Element> buffer = newBuffer(variant, CAPACITY);
        publish(buffer, variant, "ready");
        IdleStrategy idleStrategy = spy(BackoffIdleStrategy.class);

        assertThat(buffer.pollBlocking(idleStrategy).getName()).isEqualTo("ready");

        verify(idleStrategy, never()).reset();
        verify(idleStrategy, never()).idle();
    }

    @ParameterizedTest
    @MethodSource("blockingVariants")
    void pollBlockingHandsTheElementToAConsumer(Variant variant) {
        RingBuffer<Element> buffer = newBuffer(variant, CAPACITY);
        List<String> seen = new ArrayList<>();

        publishAfter(buffer, variant, "late", 250);
        buffer.pollBlocking(e -> seen.add(e.getName()), new BackoffIdleStrategy());

        assertThat(seen).containsExactly("late");
        assertThat(buffer.isEmpty()).isTrue();
    }

    @ParameterizedTest
    @MethodSource("blockingVariants")
    void pollBlockingGivesUpAtItsDeadline(Variant variant) {
        RingBuffer<Element> buffer = newBuffer(variant, CAPACITY);

        publish(buffer, variant, "present");
        assertThat(buffer.pollBlocking(new BackoffIdleStrategy(), Duration.ofSeconds(1)).getName())
                .isEqualTo("present");

        assertThat(buffer.isEmpty()).isTrue();
        assertThat(buffer.pollBlocking(new BackoffIdleStrategy(), Duration.ofMillis(10))).isNull();
    }

    @ParameterizedTest
    @MethodSource("blockingVariants")
    void offerBlockingWaitsForRoom(Variant variant) {
        RingBuffer<Element> buffer = newBuffer(variant, CAPACITY);
        for (int i = 0; i < CAPACITY; i++) {
            assertThat(publish(buffer, variant, "e" + i)).isTrue();
        }
        assertThat(buffer.isFull()).isTrue();

        IdleStrategy idleStrategy = spy(BackoffIdleStrategy.class);
        pollAfter(buffer, 250);

        publishBlocking(buffer, variant, "blocked", idleStrategy);

        verify(idleStrategy).reset();
        verify(idleStrategy, atLeast(10)).idle();
        assertThat(names(buffer::forEach)).contains("blocked");
    }

    // --- pooled instances and the translators that populate them ------------------------------------------

    @ParameterizedTest
    @MethodSource("blockingVariants")
    void offerBlockingDoesNotIdleWhenThereIsRoom(Variant variant) {
        RingBuffer<Element> buffer = newBuffer(variant, CAPACITY);
        IdleStrategy idleStrategy = spy(BackoffIdleStrategy.class);

        publishBlocking(buffer, variant, "immediate", idleStrategy);

        verify(idleStrategy, never()).reset();
        verify(idleStrategy, never()).idle();
        assertThat(buffer.poll().getName()).isEqualTo("immediate");
    }

    /**
     * The whole point of a pooled buffer: the slot keeps its instance across laps, so a producer that publishes
     * through a translator allocates nothing.
     */
    @ParameterizedTest
    @MethodSource("pooledVariants")
    void aPooledSlotKeepsItsInstanceAcrossLaps(Variant variant) {
        RingBuffer<Element> buffer = newBuffer(variant, CAPACITY);

        List<Element> pool = new ArrayList<>();
        buffer.forEachEntry(pool::add);
        assertThat(pool).hasSize(CAPACITY);

        for (int lap = 0; lap < 4; lap++) {
            for (int i = 0; i < CAPACITY; i++) {
                assertThat(buffer.offer(Element::setName, "lap" + lap + "e" + i)).isTrue();
            }
            for (int i = 0; i < CAPACITY; i++) {
                assertThat(buffer.poll().getName()).isEqualTo("lap" + lap + "e" + i);
            }
        }

        List<Element> poolAfter = new ArrayList<>();
        buffer.forEachEntry(poolAfter::add);
        // same instances, in the same slots: nothing was replaced along the way
        assertThat(poolAfter).hasSize(CAPACITY);
        for (int i = 0; i < CAPACITY; i++) {
            assertThat(poolAfter.get(i)).isSameAs(pool.get(i));
        }
    }

    @ParameterizedTest
    @MethodSource("pooledVariants")
    void everyTranslatorOverloadPopulatesTheSlotsOwnInstance(Variant variant) {
        RingBuffer<Element> buffer = newBuffer(variant, CAPACITY);

        assertThat(buffer.offer(Element::setName, "one")).isTrue();
        assertThat(buffer.offer(Element::set, "a", "b")).isTrue();
        assertThat(buffer.offer(Element::set, "a", "b", "c")).isTrue();
        assertThat(buffer.offer(Element::set, "a", "b", "c", "d")).isTrue();
        assertThat(buffer.offer(Element::set, "a", "b", "c", "d", "e")).isTrue();
        RingBuffer.EventTranslatorThreeLongArg<Element, String, String> longTranslator = Element::set;
        assertThat(buffer.offer(longTranslator, 42L, "a", "b")).isTrue();

        assertThat(buffer.poll().getName()).isEqualTo("one");
        assertThat(buffer.poll().getArgs()).containsExactly("a", "b", null, null, null);
        assertThat(buffer.poll().getArgs()).containsExactly("a", "b", "c", null, null);
        assertThat(buffer.poll().getArgs()).containsExactly("a", "b", "c", "d", null);
        assertThat(buffer.poll().getArgs()).containsExactly("a", "b", "c", "d", "e");

        Element withLong = buffer.poll();
        assertThat(withLong.getLongArg()).isEqualTo(42L);
        assertThat(withLong.getArgs()).containsExactly("a", "b", null, null, null);
    }

    @ParameterizedTest
    @MethodSource("pooledVariants")
    void aTranslatorIsNotCalledWhenThereIsNoRoom(Variant variant) {
        RingBuffer<Element> buffer = newBuffer(variant, CAPACITY);
        for (int i = 0; i < CAPACITY; i++) {
            assertThat(buffer.offer(Element::setName, "e" + i)).isTrue();
        }

        assertThat(buffer.offer((e, name) -> fail("the buffer is full, nothing to populate"), "overflow"))
                .isFalse();
    }

    @ParameterizedTest
    @MethodSource("pooledUnpaddedVariants")
    void everyBlockingTranslatorOverloadWaitsForRoom(Variant variant) {
        assertBlockingTranslatorWaits(variant,
                (buffer, idle) -> buffer.offerBlocking(Element::setName, "late", idle),
                e -> assertThat(e.getName()).isEqualTo("late"));
        assertBlockingTranslatorWaits(variant,
                (buffer, idle) -> buffer.offerBlocking(Element::set, "a", "b", idle),
                e -> assertThat(e.getArgs()).containsExactly("a", "b", null, null, null));
        assertBlockingTranslatorWaits(variant,
                (buffer, idle) -> buffer.offerBlocking(Element::set, "a", "b", "c", idle),
                e -> assertThat(e.getArgs()).containsExactly("a", "b", "c", null, null));
        assertBlockingTranslatorWaits(variant,
                (buffer, idle) -> buffer.offerBlocking(Element::set, "a", "b", "c", "d", idle),
                e -> assertThat(e.getArgs()).containsExactly("a", "b", "c", "d", null));
        assertBlockingTranslatorWaits(variant,
                (buffer, idle) -> buffer.offerBlocking(Element::set, "a", "b", "c", "d", "e", idle),
                e -> assertThat(e.getArgs()).containsExactly("a", "b", "c", "d", "e"));
        RingBuffer.EventTranslatorThreeLongArg<Element, String, String> longTranslator = Element::set;
        assertBlockingTranslatorWaits(variant,
                (buffer, idle) -> buffer.offerBlocking(longTranslator, 42L, "a", "b", idle),
                e -> assertThat(e.getLongArg()).isEqualTo(42L));
    }

    // --- helpers ------------------------------------------------------------------------------------------

    /**
     * The slot-flagged buffer reads a slot holding nothing as "no element here", which a pre-filled instance
     * would break — so it hosts no pool, and every overload that exists to populate one refuses.
     */
    @ParameterizedTest
    @MethodSource("slotFlaggedVariants")
    void theSlotFlaggedBufferRefusesEveryTranslator(Variant variant) {
        RingBuffer<Element> buffer = newBuffer(variant, CAPACITY);

        assertThrows(UnsupportedOperationException.class, () -> buffer.offer(Element::setName, "a"));
        assertThrows(UnsupportedOperationException.class, () -> buffer.offer((e, a, b) -> {
        }, "a", "b"));
        assertThrows(UnsupportedOperationException.class, () -> buffer.offer((e, a, b, c) -> {
        }, "a", "b", "c"));
        assertThrows(UnsupportedOperationException.class, () -> buffer.offer((e, a, b, c, d) -> {
        }, "a", "b", "c", "d"));
        assertThrows(UnsupportedOperationException.class, () -> buffer.offer((e, a, b, c, d, f) -> {
        }, "a", "b", "c", "d", "e"));
        assertThrows(UnsupportedOperationException.class,
                () -> buffer.offer((RingBuffer.EventTranslatorThreeLongArg<Element, String, String>) (e, l, a, b) -> {
                }, 1L, "a", "b"));
    }

    private void assertBlockingTranslatorWaits(Variant variant, BlockingPublish publish, Consumer<Element> check) {
        RingBuffer<Element> buffer = newBuffer(variant, CAPACITY);
        for (int i = 0; i < CAPACITY; i++) {
            assertThat(buffer.offer(Element::setName, "e" + i)).isTrue();
        }

        IdleStrategy idleStrategy = spy(BackoffIdleStrategy.class);
        pollAfter(buffer, 100);

        publish.publish(buffer, idleStrategy);

        verify(idleStrategy).reset();
        verify(idleStrategy, atLeast(1)).idle();

        // the element that waited is the last one in, so drain down to it
        Element last = null;
        while (buffer.isNotEmpty()) {
            last = buffer.poll();
        }
        check.accept(last);
    }

    private void publishAfter(RingBuffer<Element> buffer, Variant variant, String name, long delayMillis) {
        runAfter(delayMillis, () -> publish(buffer, variant, name));
    }

    private void pollAfter(RingBuffer<Element> buffer, long delayMillis) {
        runAfter(delayMillis, buffer::poll);
    }

    private void runAfter(long delayMillis, Runnable action) {
        Thread thread = new Thread(() -> {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(delayMillis));
            action.run();
        }, "test-kit-delayed-action");
        thread.setDaemon(true);
        thread.start();
    }

    private List<String> names(Consumer<Consumer<Element>> walk) {
        List<String> names = new ArrayList<>();
        walk.accept(e -> names.add(e.getName()));
        return names;
    }

    private int count(Consumer<Consumer<Element>> walk) {
        int[] count = {0};
        walk.accept(e -> count[0]++);
        return count[0];
    }

    @FunctionalInterface
    private interface BlockingPublish {
        void publish(RingBuffer<Element> buffer, IdleStrategy idleStrategy);
    }
}
