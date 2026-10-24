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

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.lolaf.ringos.rb.ByteRingBuffer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Several producers against the one consumer of a {@link ByteRingBuffer}: nothing lost, nothing delivered twice,
 * nothing torn, and each producer's frames in the order it wrote them, with no gap.
 */
public abstract class AbstractByteRingBufferConcurrencyTest extends AbstractByteRingBufferTestKit {

    private static final int FRAMES_PER_PRODUCER = 20_000;

    static int severalThreads() {
        return Math.max(2, Math.min(4, Runtime.getRuntime().availableProcessors() / 2));
    }

    static void startAndAwait(List<Thread> threads, CountDownLatch start) throws InterruptedException {
        threads.forEach(Thread::start);
        start.countDown();
        for (Thread thread : threads) {
            thread.join(TimeUnit.SECONDS.toMillis(60));
            assertThat(thread.isAlive()).as(thread.getName() + " still running").isFalse();
        }
    }

    /**
     * @param capacity a tight one keeps the producers meeting a full buffer and the wrap; a roomy one lets them run
     */
    @ParameterizedTest
    @ValueSource(ints = {1024, 1 << 16})
    @Timeout(120)
    void producersLoseNothingAndKeepTheirOrder(int capacity) throws InterruptedException {
        ByteRingBuffer queue = byteRingBufferBuilder().buildQueue(capacity);
        int producers = severalThreads();
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            int producer = p;
            threads.add(new Thread(() -> {
                Frame frame = new Frame();
                await(start);
                for (int sequence = 0; sequence < FRAMES_PER_PRODUCER; sequence++) {
                    int length = payloadLength(producer, sequence, queue.getMaxPayloadLength());
                    while (!queue.write(producer, length, FILL, frame.of(producer, sequence))) {
                        Thread.onSpinWait();
                    }
                }
            }, "producer-" + p));
        }

        int[] next = new int[producers];
        long[] received = new long[1];
        AtomicReference<String> failure = new AtomicReference<>();
        Thread consumer = new Thread(() -> {
            await(start);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (received[0] < (long) producers * FRAMES_PER_PRODUCER && System.nanoTime() < deadline) {
                int read = queue.read((type, buffer, index, length) -> {
                    received[0]++;
                    int producer = buffer.getInt(index);
                    int sequence = buffer.getInt(index + 4);
                    if (!intact(buffer, index, length) || type != producer || sequence != next[producer]
                            || length != payloadLength(producer, sequence, queue.getMaxPayloadLength())) {
                        failure.compareAndSet(null, "frame " + received[0] + ": type " + type + ", producer " + producer
                                + ", sequence " + sequence + " where " + next[producer] + " was expected, length " + length);
                    }
                    next[producer] = sequence + 1;
                }, 64);
                if (read == 0) {
                    Thread.onSpinWait();
                }
            }
        }, "consumer");
        threads.add(consumer);

        startAndAwait(threads, start);

        assertThat(failure.get()).isNull();
        assertThat(received[0]).isEqualTo((long) producers * FRAMES_PER_PRODUCER);
        assertThat(queue.getSize()).isZero();
    }

    static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
