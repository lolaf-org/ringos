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

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.lolaf.ringos.idling.BackoffIdleStrategy;
import org.lolaf.ringos.idling.IdleStrategy;
import org.lolaf.ringos.rb.RingBuffer;
import org.lolaf.ringos.rb.RingBufferFactory.AccessType;

import java.time.Duration;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * The harness the four producer/consumer test classes run their load through, and the invariants it holds every
 * implementation to.
 *
 * <p>Each element is one {@code long} carrying both a producer id and that producer's own sequence number, which
 * is what lets a consumer check ordering without the recording itself becoming a synchronisation point: every
 * consumer keeps its own {@link BitSet} per producer and its own high-water mark per producer, and nothing is
 * merged until every thread has been joined. A test whose bookkeeping is shared between the consumer threads
 * tends to hide the very reordering it was written to find.
 *
 * <p>Three invariants are checked after every run, and they are the ones a broken compare-and-swap or a missing
 * fence actually violates:
 * <ul>
 *   <li><b>Nothing is lost and nothing is delivered twice</b> — the merged receipts are exactly the elements
 *       published, counted and as a set.</li>
 *   <li><b>Each producer's elements reach each consumer in the order that producer published them.</b> A
 *       consumer sees a subsequence of the order the elements were claimed in, and a single producer claims in
 *       program order, so its own sequence numbers must climb in every consumer's stream however many consumers
 *       there are. {@link #requireConsecutive()} tightens this to "with no gaps" where a single consumer makes
 *       that true.</li>
 *   <li><b>The buffer ends drained and consistent</b> — empty, size zero, head and tail both equal to the number
 *       of elements published.</li>
 * </ul>
 *
 * @see AbstractRingBufferContractTest for what one thread must be able to expect of a buffer
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractRingBufferConcurrencyTest extends AbstractRingBufferTestKit {

    /**
     * A capacity small enough that the buffer is nearly always at one end or the other, which is where the claim
     * protocols have to be right. Most of the load runs at this size on purpose.
     */
    protected static final int TIGHT_CAPACITY = 8;

    /**
     * A capacity that lets producers and consumers run without meeting at the ends of the buffer for a while,
     * so batch claims have something to claim.
     */
    protected static final int ROOMY_CAPACITY = 1024;

    /**
     * How long a run may take before it is called a failure rather than waited on any longer. Generous: it only
     * has to beat a hang, and a lost element shows up as this timeout with the counts printed.
     */
    protected static final Duration RUN_TIMEOUT = Duration.ofSeconds(60);

    /**
     * Publishes into a pooled slot without boxing the value, which is the overload a real producer of primitives
     * would use. The two reference arguments have nothing to carry here.
     */
    private static final RingBuffer.EventTranslatorThreeLongArg<Payload, Void, Void> PUBLISH_VALUE =
            (payload, value, unusedA, unusedB) -> payload.value = value;

    /**
     * @return however many threads of one side to run, kept modest so the run is a concurrency test and not a
     * benchmark of the machine it happens to be on
     */
    protected static int severalThreads() {
        return Math.max(2, Math.min(4, Runtime.getRuntime().availableProcessors() / 2));
    }

    private static long encode(int producerId, int sequence) {
        return ((long) producerId << 32) | sequence;
    }

    /**
     * @return the access type the buffers under test are built for
     */
    protected abstract AccessType accessType();

    /**
     * @return how many producer threads to run; {@code 1} for the single-producer access types
     */
    protected abstract int producerCount();

    /**
     * @return how many consumer threads to run; {@code 1} for the single-consumer access types
     */
    protected abstract int consumerCount();

    /**
     * Whether a consumer must see a producer's sequence numbers with no gaps, rather than merely climbing.
     * That is only true when this test's shape leaves a producer's elements nowhere else to go — one consumer —
     * so it is off by default and the single-consumer classes turn it on.
     *
     * @return {@code true} to require gapless per-producer sequences in every consumer's stream
     */
    protected boolean requireConsecutive() {
        return consumerCount() == 1;
    }

    // --- producing ----------------------------------------------------------------------------------------

    /**
     * Runs one load and asserts the three invariants.
     *
     * @param variant     the buffer shape to run against; its access type must be {@link #accessType()}
     * @param capacity    number of slots — a small one keeps the buffer swinging between full and empty, which is
     *                    where the claim protocols are hardest
     * @param perProducer how many elements each producer publishes
     * @param offerStyle  how producers publish
     * @param pollStyle   how consumers consume
     * @param pacing      whether either side is deliberately held back
     */
    protected void runLoad(Variant variant, int capacity, int perProducer,
                           OfferStyle offerStyle, PollStyle pollStyle, Pacing pacing) {
        checkCombinationIsLegal(variant, offerStyle, pollStyle);

        int producers = producerCount();
        int consumers = consumerCount();
        long total = (long) producers * perProducer;
        RingBuffer<Payload> buffer = newBuffer(variant, capacity, Payload::new);

        CountDownLatch startLine = new CountDownLatch(1);
        CountDownLatch producersDone = new CountDownLatch(producers);
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        long deadline = System.nanoTime() + RUN_TIMEOUT.toNanos();

        List<Thread> threads = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            int producerId = p;
            threads.add(thread("producer-" + p, firstFailure, () -> {
                await(startLine);
                try {
                    produce(buffer, producerId, perProducer, offerStyle, pacing, deadline);
                } finally {
                    producersDone.countDown();
                }
            }));
        }

        List<Recorder> recorders = new ArrayList<>();
        for (int c = 0; c < consumers; c++) {
            Recorder recorder = new Recorder(producers, perProducer, requireConsecutive());
            recorders.add(recorder);
            threads.add(thread("consumer-" + c, firstFailure, () -> {
                await(startLine);
                consume(buffer, recorder, pollStyle, pacing, producersDone, deadline);
            }));
        }

        threads.forEach(Thread::start);
        startLine.countDown();
        joinAll(threads);

        if (firstFailure.get() != null) {
            fail("a worker thread failed", firstFailure.get());
        }
        assertInvariants(buffer, recorders, producers, perProducer, total);
    }

    // --- consuming ----------------------------------------------------------------------------------------

    private void assertInvariants(RingBuffer<Payload> buffer, List<Recorder> recorders,
                                  int producers, int perProducer, long total) {
        for (Recorder recorder : recorders) {
            if (recorder.violation != null) {
                fail(recorder.violation);
            }
        }

        long received = recorders.stream().mapToLong(r -> r.count).sum();
        assertThat(received).as("every published element must be consumed exactly once").isEqualTo(total);

        for (int p = 0; p < producers; p++) {
            BitSet merged = new BitSet(perProducer);
            for (Recorder recorder : recorders) {
                BitSet mine = recorder.received[p];
                if (merged.intersects(mine)) {
                    BitSet duplicates = (BitSet) merged.clone();
                    duplicates.and(mine);
                    fail("producer " + p + " had elements delivered to more than one consumer: "
                            + duplicates.stream().limit(10).boxed().collect(java.util.stream.Collectors.toList()));
                }
                merged.or(mine);
            }
            assertThat(merged.cardinality())
                    .as("producer %d must have every one of its elements delivered exactly once", p)
                    .isEqualTo(perProducer);
        }

        assertThat(buffer.isEmpty()).as("the buffer must end drained").isTrue();
        assertThat(buffer.getSize()).isZero();
        assertThat(buffer.getCurrentTail()).as("tail counts what was published").isEqualTo(total);
        assertThat(buffer.getCurrentHead()).as("head counts what was consumed").isEqualTo(total);
    }

    private void produce(RingBuffer<Payload> buffer, int producerId, int perProducer,
                         OfferStyle offerStyle, Pacing pacing, long deadline) {
        IdleStrategy idleStrategy = new BackoffIdleStrategy();
        for (int sequence = 0; sequence < perProducer; sequence++) {
            long value = encode(producerId, sequence);
            switch (offerStyle) {
                case OFFER:
                    while (!buffer.offer(new Payload(value))) {
                        stall(idleStrategy, deadline, "producer " + producerId + " could not publish");
                    }
                    break;
                case TRANSLATOR:
                    while (!buffer.offer(PUBLISH_VALUE, value, null, null)) {
                        stall(idleStrategy, deadline, "producer " + producerId + " could not publish");
                    }
                    break;
                case OFFER_BLOCKING:
                    buffer.offerBlocking(new Payload(value), idleStrategy);
                    break;
                default:
                    throw new IllegalStateException("unhandled offer style " + offerStyle);
            }
            idleStrategy.reset();
            if (pacing == Pacing.SLOW_PRODUCERS && (sequence & 0x3F) == 0) {
                LockSupport.parkNanos(1_000L);
            }
        }
    }

    private void consume(RingBuffer<Payload> buffer, Recorder recorder, PollStyle pollStyle, Pacing pacing,
                         CountDownLatch producersDone, long deadline) {
        Consumer<Payload> record = payload -> recorder.record(payload.value);
        Consumer<Payload>[] batch = batchConsumers(record);
        IdleStrategy idleStrategy = new BackoffIdleStrategy();
        long paced = 0;

        while (!isDrained(buffer, producersDone)) {
            if (System.nanoTime() > deadline) {
                // the final assertions report what was actually delivered, which says far more than a hang
                return;
            }
            int consumed = consumeOnce(buffer, recorder, record, batch, pollStyle, idleStrategy);
            if (consumed == 0) {
                idleStrategy.idle();
            } else {
                idleStrategy.reset();
                if (pacing == Pacing.SLOW_CONSUMERS && ((paced += consumed) & 0x3F) < consumed) {
                    LockSupport.parkNanos(1_000L);
                }
            }
        }
    }

    // --- shapes and guards --------------------------------------------------------------------------------

    private int consumeOnce(RingBuffer<Payload> buffer, Recorder recorder, Consumer<Payload> record,
                            Consumer<Payload>[] batch, PollStyle pollStyle, IdleStrategy idleStrategy) {
        switch (pollStyle) {
            case POLL: {
                Payload payload = buffer.poll();
                if (payload == null) {
                    return 0;
                }
                recorder.record(payload.value);
                return 1;
            }
            case POLL_CONSUMER:
                return buffer.poll(record) ? 1 : 0;
            case BATCH:
                return buffer.poll(batch);
            case DRAIN: {
                int[] drained = {0};
                buffer.drain(payload -> {
                    recorder.record(payload.value);
                    drained[0]++;
                });
                return drained[0];
            }
            case POLL_BLOCKING: {
                // the bounded overload, so a consumer that has run out of work still notices the producers ended
                Payload payload = buffer.pollBlocking(idleStrategy, Duration.ofMillis(50));
                if (payload == null) {
                    return 0;
                }
                recorder.record(payload.value);
                return 1;
            }
            default:
                throw new IllegalStateException("unhandled poll style " + pollStyle);
        }
    }

    /**
     * Once the producers have all returned the tail is final, so an empty buffer really does mean every element
     * has been claimed by some consumer — whether or not that consumer has recorded it yet, which the joins
     * downstream take care of.
     */
    private boolean isDrained(RingBuffer<Payload> buffer, CountDownLatch producersDone) {
        return producersDone.getCount() == 0 && buffer.isEmpty();
    }

    /**
     * Rejects the combinations that are not the implementation's to honour, rather than letting them fail as if
     * they were bugs.
     */
    private void checkCombinationIsLegal(Variant variant, OfferStyle offerStyle, PollStyle pollStyle) {
        if (variant.getAccessType() != accessType()) {
            throw new IllegalArgumentException(
                    "this test runs " + accessType() + " buffers, not " + variant.getAccessType());
        }
        if (offerStyle == OfferStyle.TRANSLATOR && !variant.isPooled()) {
            throw new IllegalArgumentException("a translator needs a pooled instance to populate");
        }
        if (variant.isPooled() && (pollStyle == PollStyle.POLL || pollStyle == PollStyle.POLL_BLOCKING)) {
            // the element-returning overloads release the slot before the caller reads it, so on a pooled buffer
            // a producer may overwrite the instance under the consumer's feet. That is the documented ownership
            // rule, not a defect, so the kit consumes pooled buffers inside a callback instead.
            throw new IllegalArgumentException(
                    "a pooled buffer must be consumed inside a callback, not through an element-returning poll");
        }
    }

    @SuppressWarnings("unchecked")
    private Consumer<Payload>[] batchConsumers(Consumer<Payload> record) {
        Consumer<Payload>[] batch = new Consumer[4];
        java.util.Arrays.fill(batch, record);
        return batch;
    }

    private void stall(IdleStrategy idleStrategy, long deadline, String what) {
        if (System.nanoTime() > deadline) {
            throw new IllegalStateException(what + " before the run's deadline");
        }
        idleStrategy.idle();
    }

    private Thread thread(String name, AtomicReference<Throwable> firstFailure, Runnable body) {
        Thread thread = new Thread(body, name);
        thread.setDaemon(true);
        thread.setUncaughtExceptionHandler((t, error) -> firstFailure.compareAndSet(null, error));
        return thread;
    }

    private void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted at the start line", e);
        }
    }

    private void joinAll(List<Thread> threads) {
        long deadlineMillis = System.currentTimeMillis() + RUN_TIMEOUT.toMillis() + TimeUnit.SECONDS.toMillis(5);
        for (Thread thread : threads) {
            long remaining = Math.max(1L, deadlineMillis - System.currentTimeMillis());
            try {
                thread.join(remaining);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while joining " + thread.getName(), e);
            }
            assertThat(thread.isAlive()).as("%s did not finish", thread.getName()).isFalse();
        }
    }

    /**
     * @return how many elements each producer publishes in the main load runs
     */
    protected int perProducer() {
        return 20_000;
    }

    /**
     * The combinations of buffer shape and API worth running the full load through: each shape of the access
     * type under test, against the publish and consume styles that shape actually supports.
     *
     * @return one case per (variant, offer style, poll style, capacity)
     */
    protected Stream<Arguments> loadCases() {
        List<Arguments> cases = new ArrayList<>();
        for (Variant variant : Variant.of(accessType()).collect(Collectors.toList())) {
            if (variant.isPooled()) {
                cases.add(Arguments.of(variant, OfferStyle.TRANSLATOR, PollStyle.POLL_CONSUMER, TIGHT_CAPACITY));
                cases.add(Arguments.of(variant, OfferStyle.TRANSLATOR, PollStyle.BATCH, ROOMY_CAPACITY));
                cases.add(Arguments.of(variant, OfferStyle.TRANSLATOR, PollStyle.DRAIN, TIGHT_CAPACITY));
            } else {
                cases.add(Arguments.of(variant, OfferStyle.OFFER, PollStyle.POLL, TIGHT_CAPACITY));
                cases.add(Arguments.of(variant, OfferStyle.OFFER, PollStyle.BATCH, ROOMY_CAPACITY));
                cases.add(Arguments.of(variant, OfferStyle.OFFER, PollStyle.DRAIN, TIGHT_CAPACITY));
                cases.add(Arguments.of(variant, OfferStyle.OFFER_BLOCKING, PollStyle.POLL_BLOCKING, TIGHT_CAPACITY));
            }
        }
        return cases.stream();
    }

    /**
     * @return one case per buffer shape, each with the publish and consume style that shape is meant to be used
     * with, for the runs where the interest is the pacing rather than the API
     */
    protected Stream<Arguments> pacedCases() {
        return Variant.of(accessType()).map(variant -> variant.isPooled()
                ? Arguments.of(variant, OfferStyle.TRANSLATOR, PollStyle.POLL_CONSUMER)
                : Arguments.of(variant, OfferStyle.OFFER, PollStyle.POLL));
    }

    @ParameterizedTest(name = "{0}, {1} then {2}, capacity {3}")
    @MethodSource("loadCases")
    @Timeout(value = 120)
    void deliversEveryElementExactlyOnceAndInOrder(Variant variant, OfferStyle offerStyle, PollStyle pollStyle,
                                                   int capacity) {
        runLoad(variant, capacity, perProducer(), offerStyle, pollStyle, Pacing.FLAT_OUT);
    }

    /**
     * Consumers that cannot keep up leave the buffer full, which is the state that makes producers contend on
     * the claim and refresh whatever they cache about the consumers' position.
     */
    @ParameterizedTest(name = "{0}, {1} then {2}")
    @MethodSource("pacedCases")
    @Timeout(value = 120)
    void survivesConsumersThatCannotKeepUp(Variant variant, OfferStyle offerStyle, PollStyle pollStyle) {
        runLoad(variant, TIGHT_CAPACITY, 2_000, offerStyle, pollStyle, Pacing.SLOW_CONSUMERS);
    }

    /**
     * The mirror image: a buffer that spends the run empty, so consumers keep finding a slot whose element has
     * been claimed but not yet published.
     */
    @ParameterizedTest(name = "{0}, {1} then {2}")
    @MethodSource("pacedCases")
    @Timeout(value = 120)
    void survivesProducersThatCannotKeepUp(Variant variant, OfferStyle offerStyle, PollStyle pollStyle) {
        runLoad(variant, ROOMY_CAPACITY, 2_000, offerStyle, pollStyle, Pacing.SLOW_PRODUCERS);
    }

    /**
     * How a producer publishes.
     */
    protected enum OfferStyle {
        /**
         * {@link RingBuffer#offer(Object)}, storing a fresh reference.
         */
        OFFER,
        /**
         * An {@code EventTranslator} populating the slot's pooled instance; pooled buffers only.
         */
        TRANSLATOR,
        /**
         * {@link RingBuffer#offerBlocking(Object, IdleStrategy)}, idling rather than retrying by hand.
         */
        OFFER_BLOCKING
    }

    /**
     * How a consumer consumes.
     */
    protected enum PollStyle {
        /**
         * {@link RingBuffer#poll()}; unpooled buffers only, as it releases the slot before returning.
         */
        POLL,
        /**
         * {@link RingBuffer#poll(Consumer)}, which holds the slot for the length of the callback.
         */
        POLL_CONSUMER,
        /**
         * {@link RingBuffer#poll(Consumer[])}, claiming several elements per head update.
         */
        BATCH,
        /**
         * {@link RingBuffer#drain(Consumer)}, consuming until the buffer looks empty.
         */
        DRAIN,
        /**
         * The bounded {@link RingBuffer#pollBlocking(IdleStrategy, Duration)}; unpooled buffers only.
         */
        POLL_BLOCKING
    }

    /**
     * Which side of the buffer is deliberately held back, and thereby whether the buffer spends the run full,
     * empty or somewhere in between. The three keep different branches of the claim protocols hot — a full
     * buffer is what makes a producer refresh its cached view of the consumer's position, an empty one is what
     * makes a consumer keep finding an unpublished slot.
     */
    protected enum Pacing {
        /**
         * Neither side waits; the buffer swings between full and empty at whatever rate the machine allows.
         */
        FLAT_OUT,
        /**
         * Producers pause periodically, so consumers mostly find the buffer empty.
         */
        SLOW_PRODUCERS,
        /**
         * Consumers pause periodically, so producers mostly find the buffer full.
         */
        SLOW_CONSUMERS
    }

    /**
     * The element the load runs on: mutable, so a pooled slot can hold one and have a translator write into it.
     */
    protected static final class Payload {
        long value;

        Payload() {
        }

        Payload(long value) {
            this.value = value;
        }
    }

    /**
     * One consumer thread's private tally. Nothing here is shared, so recording costs no coordination and cannot
     * mask a reordering the buffer actually performed.
     */
    private static final class Recorder {

        private final BitSet[] received;
        private final long[] lastSeen;
        private final boolean requireConsecutive;
        private long count;
        private String violation;

        Recorder(int producers, int perProducer, boolean requireConsecutive) {
            this.received = new BitSet[producers];
            this.lastSeen = new long[producers];
            this.requireConsecutive = requireConsecutive;
            for (int p = 0; p < producers; p++) {
                received[p] = new BitSet(perProducer);
                lastSeen[p] = -1L;
            }
        }

        void record(long value) {
            int producerId = (int) (value >>> 32);
            int sequence = (int) value;
            long previous = lastSeen[producerId];

            if (sequence <= previous) {
                violation("producer " + producerId + " reached this consumer out of order: " + sequence
                        + " arrived after " + previous);
            } else if (requireConsecutive && sequence != previous + 1) {
                violation("producer " + producerId + " skipped a sequence at this consumer: " + sequence
                        + " arrived after " + previous);
            }

            lastSeen[producerId] = sequence;
            received[producerId].set(sequence);
            count++;
        }

        private void violation(String message) {
            if (violation == null) {
                violation = message;
            }
        }
    }
}
