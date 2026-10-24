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
import org.lolaf.ringos.rb.BroadcastByteRingBuffer;
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
 * Several producers and several readers of a {@link BroadcastByteRingBuffer}. A reader is never handed a torn frame,
 * never sees a producer's frames out of order, and is told when it was lapped; one that keeps up loses nothing.
 */
public abstract class AbstractBroadcastByteRingBufferConcurrencyTest extends AbstractByteRingBufferTestKit {

    /**
     * Below the largest payload of the smallest ring, so a roomy ring really does hold every frame written.
     */
    private static final int MAX_PAYLOAD = 96;

    /**
     * Room for everything the producers write, so no reader can be lapped.
     */
    @Test
    @Timeout(120)
    void readersThatAreNeverLappedGetEveryFrameInEachProducersOrder() throws InterruptedException {
        int framesPerProducer = 5_000;
        BroadcastByteRingBuffer ring = byteRingBufferBuilder().buildBroadcast(1 << 22);
        int producers = severalThreads();
        List<ReaderRun> readers = List.of(new ReaderRun(ring, producers, 0), new ReaderRun(ring, producers, 0),
                new ReaderRun(ring, producers, 0));

        run(ring, producers, framesPerProducer, readers);

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
        BroadcastByteRingBuffer ring = byteRingBufferBuilder().buildBroadcast(1024);
        int producers = severalThreads();
        ReaderRun fast = new ReaderRun(ring, producers, 0);
        ReaderRun slow = new ReaderRun(ring, producers, TimeUnit.MICROSECONDS.toNanos(50));

        run(ring, producers, 50_000, List.of(fast, slow));

        assertThat(fast.failure.get()).isNull();
        assertThat(slow.failure.get()).isNull();
        assertThat(slow.laps).isPositive();
        assertThat(slow.frames).isPositive();
    }

    private void run(BroadcastByteRingBuffer ring, int producers, int framesPerProducer, List<ReaderRun> readers)
            throws InterruptedException {
        CountDownLatch start = new CountDownLatch(1);
        AtomicBoolean producersDone = new AtomicBoolean();
        CountDownLatch producing = new CountDownLatch(producers);
        List<Thread> threads = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            int producer = p;
            threads.add(new Thread(() -> {
                Frame frame = new Frame();
                await(start);
                for (int sequence = 0; sequence < framesPerProducer; sequence++) {
                    ring.write(producer, payloadLength(producer, sequence, MAX_PAYLOAD), FILL,
                            frame.of(producer, sequence));
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
        private final int[] last;
        private final long pauseNanos;
        private final AtomicReference<String> failure = new AtomicReference<>();
        private long frames;
        private long laps;
        private long gaps;

        ReaderRun(BroadcastByteRingBuffer ring, int producers, long pauseNanos) {
            this.ring = ring;
            this.reader = ring.newReader();
            this.dst = ByteBuffer.allocate(MAX_PAYLOAD);
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
                        return;
                    }
                    Thread.onSpinWait();
                } else {
                    check(length);
                    if (pauseNanos > 0 && frames % 64 == 0) {
                        LockSupport.parkNanos(pauseNanos);
                    }
                }
            }
            failure.compareAndSet(null, "still reading at the deadline after " + frames + " frames");
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
