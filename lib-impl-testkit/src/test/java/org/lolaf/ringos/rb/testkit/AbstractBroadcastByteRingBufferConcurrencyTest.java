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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.lolaf.ringos.idling.RetryStrategy;
import org.lolaf.ringos.idling.YieldingIdleStrategy;
import org.lolaf.ringos.rb.BroadcastByteRingBuffer;
import org.lolaf.ringos.rb.ProducerType;
import org.lolaf.ringos.rb.ByteRingReader;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.lolaf.ringos.rb.testkit.AbstractByteRingBufferConcurrencyTest.await;
import static org.lolaf.ringos.rb.testkit.AbstractByteRingBufferConcurrencyTest.severalThreads;
import static org.lolaf.ringos.rb.testkit.AbstractByteRingBufferConcurrencyTest.startAndAwait;

/**
 * Producers, as many as the {@link ProducerType} allows, and several readers of a {@link BroadcastByteRingBuffer}. A
 * reader is never handed a torn frame, never sees a producer's frames out of order, and is told when it was lapped;
 * one that keeps up loses nothing, and so does a gating one, however slow, when the producers write retrying.
 */
public abstract class AbstractBroadcastByteRingBufferConcurrencyTest extends AbstractByteRingBufferTestKit {

    /**
     * Below the largest payload of the smallest ring, so a roomy ring really does hold every frame written.
     */
    private static final int MAX_PAYLOAD = 96;

    protected abstract ProducerType producers();

    private int producerCount() {
        return producers() == ProducerType.SINGLE ? 1 : severalThreads();
    }

    /**
     * Room for everything the producers write, so no reader can be lapped.
     */
    @Test
    @Timeout(120)
    void readersThatAreNeverLappedGetEveryFrameInEachProducersOrder() throws InterruptedException {
        int framesPerProducer = 5_000;
        BroadcastByteRingBuffer ring = byteRingBufferBuilder().buildBroadcast(1 << 22, producers());
        int producers = producerCount();
        List<ReaderRun> readers = List.of(new ReaderRun(ring, producers, 0), new ReaderRun(ring, producers, 0),
                new ReaderRun(ring, producers, 0));

        run(ring, producers, framesPerProducer, readers, false);

        for (ReaderRun reader : readers) {
            assertThat(reader.failure.get()).isNull();
            assertThat(reader.laps).isZero();
            assertThat(reader.frames).isEqualTo((long) producers * framesPerProducer);
            assertThat(reader.gaps).as("gaps in a producer's sequence").isZero();
        }
    }

    /**
     * A buffer small enough that a slow reader is lapped again and again, while a fast one mostly keeps up.
     */
    @Test
    @Timeout(120)
    void aLappedReaderIsToldAndNeverHandedATornFrame() throws InterruptedException {
        BroadcastByteRingBuffer ring = byteRingBufferBuilder().buildBroadcast(1024, producers());
        int producers = producerCount();
        ReaderRun fast = new ReaderRun(ring, producers, 0);
        ReaderRun slow = new ReaderRun(ring, producers, TimeUnit.MICROSECONDS.toNanos(50));

        run(ring, producers, 50_000, List.of(fast, slow), false);

        assertThat(fast.failure.get()).isNull();
        assertThat(slow.failure.get()).isNull();
        assertThat(slow.laps).isPositive();
        assertThat(slow.frames).isPositive();
    }

    /**
     * The same small buffer, the producers retrying: the slow reader gates them, so it is never lapped, gets every
     * frame, and finds each frame it unreads unchanged; a reader that does not gate beside it may still be lapped.
     */
    @Test
    @Timeout(120)
    void aSlowGatingReaderHoldsRetryingWritesBackAndLosesNothing() throws InterruptedException {
        BroadcastByteRingBuffer ring = byteRingBufferBuilder().buildBroadcast(1024, producers());
        int producers = producerCount();
        int framesPerProducer = 20_000;
        ReaderRun gating = new ReaderRun(ring, producers, TimeUnit.MICROSECONDS.toNanos(50), true);
        ReaderRun plain = new ReaderRun(ring, producers, TimeUnit.MICROSECONDS.toNanos(50), false);

        run(ring, producers, framesPerProducer, List.of(gating, plain), true);

        assertThat(gating.failure.get()).isNull();
        assertThat(gating.laps).isZero();
        assertThat(gating.frames).isEqualTo((long) producers * framesPerProducer);
        assertThat(gating.gaps).as("gaps in a producer's sequence").isZero();
        assertThat(gating.unreads).isPositive();
        assertThat(plain.failure.get()).isNull();
    }

    private void run(BroadcastByteRingBuffer ring, int producers, int framesPerProducer, List<ReaderRun> readers,
                     boolean retrying) throws InterruptedException {
        CountDownLatch start = new CountDownLatch(1);
        AtomicBoolean producersDone = new AtomicBoolean();
        CountDownLatch producing = new CountDownLatch(producers);
        List<Thread> threads = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            int producer = p;
            threads.add(new Thread(() -> {
                Frame frame = new Frame();
                RetryStrategy retryForever = RetryStrategy.idlingWhile(YieldingIdleStrategy.getInstance(), () -> true);
                await(start);
                for (int sequence = 0; sequence < framesPerProducer; sequence++) {
                    int length = payloadLength(producer, sequence, MAX_PAYLOAD);
                    if (retrying) {
                        ring.writeRetrying(producer, length, FILL, frame.of(producer, sequence), retryForever);
                    } else {
                        ring.write(producer, length, FILL, frame.of(producer, sequence));
                    }
                }
                producing.countDown();
                if (producing.getCount() == 0) {
                    producersDone.set(true);
                }
            }, "producer-" + p));
        }
        for (int r = 0; r < readers.size(); r++) {
            ReaderRun reader = readers.get(r);
            threads.add(new Thread(() -> {
                await(start);
                reader.run(producersDone);
            }, "reader-" + r));
        }
        startAndAwait(threads, start);
    }

    private static final class ReaderRun {

        private final ByteRingReader reader;
        private final BroadcastByteRingBuffer ring;
        private final ByteBuffer dst;
        private final ByteBuffer reread;
        private final boolean gating;
        private final int[] last;
        private final long pauseNanos;
        private final AtomicReference<String> failure = new AtomicReference<>();
        private long frames;
        private long laps;
        private long gaps;
        private long unreads;

        ReaderRun(BroadcastByteRingBuffer ring, int producers, long pauseNanos) {
            this(ring, producers, pauseNanos, false);
        }

        ReaderRun(BroadcastByteRingBuffer ring, int producers, long pauseNanos, boolean gating) {
            this.ring = ring;
            this.gating = gating;
            this.reader = gating ? ring.newGatingReader() : ring.newReader();
            this.dst = ByteBuffer.allocate(MAX_PAYLOAD);
            this.reread = ByteBuffer.allocate(MAX_PAYLOAD);
            this.last = new int[producers];
            Arrays.fill(last, -1);
            this.pauseNanos = pauseNanos;
        }

        void run(AtomicBoolean producersDone) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (System.nanoTime() < deadline) {
                int length = reader.readInto(dst, 0);
                if (length == ByteRingReader.LAPPED) {
                    laps++;
                    reader.seekToTail();
                } else if (length == ByteRingReader.NO_FRAME) {
                    if (producersDone.get() && reader.getPosition() == ring.getTailPosition()) {
                        reader.close();
                        return;
                    }
                    Thread.onSpinWait();
                } else {
                    check(length);
                    if (gating && frames % 97 == 0) {
                        checkUnread(length);
                    }
                    if (pauseNanos > 0 && frames % 64 == 0) {
                        LockSupport.parkNanos(pauseNanos);
                    }
                }
            }
            failure.compareAndSet(null, "still reading at the deadline after " + frames + " frames");
        }

        private void checkUnread(int length) {
            unreads++;
            long position = reader.getFramePosition();
            if (!reader.unread()) {
                failure.compareAndSet(null, "a gating reader could not unread the frame at " + position);
                return;
            }
            if (reader.readInto(reread, 0) != length || reader.getFramePosition() != position
                    || !sameBytes(reread, dst, length)) {
                failure.compareAndSet(null, "the frame unread at " + position + " changed");
            }
        }

        private static boolean sameBytes(ByteBuffer a, ByteBuffer b, int length) {
            for (int i = 0; i < length; i++) {
                if (a.get(i) != b.get(i)) {
                    return false;
                }
            }
            return true;
        }

        private void check(int length) {
            frames++;
            int producer = dst.getInt(0);
            int sequence = dst.getInt(4);
            if (!intact(dst, 0, length) || reader.getType() != producer
                    || length != payloadLength(producer, sequence, MAX_PAYLOAD)) {
                failure.compareAndSet(null, "torn frame at " + reader.getFramePosition() + ": type " + reader.getType()
                        + ", producer " + producer + ", sequence " + sequence + ", length " + length);
                return;
            }
            if (sequence <= last[producer]) {
                failure.compareAndSet(null, "producer " + producer + " sequence " + sequence + " after " + last[producer]);
            }
            if (sequence != last[producer] + 1) {
                gaps++;
            }
            last[producer] = sequence;
        }
    }
}
