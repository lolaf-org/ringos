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
package org.lolaf.ringos.rb.spi;

import org.lolaf.ringos.rb.RingBuffer;

import jdk.internal.vm.annotation.Contended;
import org.lolaf.ringos.unsafe.UnsafeOperations;
import org.lolaf.ringos.unsafe.UnsafeOperationsApi;
import org.lolaf.ringos.idling.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.IntFunction;


public abstract class AbstractRingBuffer<T> implements RingBuffer<T> {
    public static final int BUFFER_PAD = 32;

    static {
        checkContendedPaddingIsInEffect(LoggerFactory.getLogger(AbstractRingBuffer.class));
    }

    protected final T[] buffer;
    protected final int capacity;
    protected final int mask;
    protected final int bufferPadding;
    protected boolean clearElementOnPoll;
    @Contended
    protected long head;
    @Contended
    protected long tail;

    @SuppressWarnings("unchecked")
    protected AbstractRingBuffer(int capacity, boolean bufferPaddingEnabled, IntFunction<T> elementInstanceProducer) {
        if (!isPowerOfTwo(capacity)) {
            throw new IllegalArgumentException("Capacity " + capacity + " must be a power of 2");
        }
        this.bufferPadding = bufferPaddingEnabled ? BUFFER_PAD : 0;
        this.buffer = (T[]) new Object[capacity + (2 * bufferPadding)];
        this.capacity = capacity;
        this.mask = capacity - 1;
        if (elementInstanceProducer != null) {
            for (int i = 0; i < capacity; i++) {
                buffer[i + this.bufferPadding] = elementInstanceProducer.apply(i);
            }
        }
        this.clearElementOnPoll = elementInstanceProducer == null;
    }

    /**
     * Checks that {@link #head} and {@link #tail} really did land on separate cache lines, rather than checking for
     * the JVM flags that would have put them there.
     *
     * <p>The flags are the means and the layout is the end, and reading the flags back is the less reliable of the
     * two: {@code JAVA_TOOL_OPTIONS}, an {@code @argfile} and a container's own JAVA_OPTS all set them without
     * appearing on the command line, and {@link ProcessHandle} reports no command line at all on a good number of
     * Linux setups. Measuring the layout answers the only question worth asking, however the JVM was configured —
     * and answers it for {@code -XX:-EnableContended} or a JDK that has moved the annotation too, neither of which
     * a flag check would notice.
     */
    private static void checkContendedPaddingIsInEffect(Logger log) {
        if (!UnsafeOperationsApi.isAvailable()) {
            log.info("Unable to read this JVM's field layout, so @Contended padding could not be verified; set"
                    + " -XX:-RestrictContended and -XX:ContendedPaddingWidth=<your L1 cache line size> for"
                    + " optimal performance");
            return;
        }
        UnsafeOperationsApi.ifAvailableDo(uo -> {
            long headOffset = uo.objectFieldOffset(AbstractRingBuffer.class, "head");
            long tailOffset = uo.objectFieldOffset(AbstractRingBuffer.class, "tail");
            int cacheLineSize = uo.getL1CacheLineSize();
            if (headOffset == UnsafeOperations.UNKNOWN_FIELD_OFFSET
                    || tailOffset == UnsafeOperations.UNKNOWN_FIELD_OFFSET) {
                log.info("Unable to read the ring buffer's field layout, so @Contended padding could not be"
                        + " verified; set -XX:-RestrictContended and -XX:ContendedPaddingWidth={} for"
                        + " optimal performance", cacheLineSize);
                return;
            }
            // the JVM lays fields out as it pleases, so neither offset is necessarily the lower of the two
            long separation = Math.abs(tailOffset - headOffset);
            if (separation < cacheLineSize) {
                // over-padding is not reported: a width wider than the line wastes memory but costs no contention
                log.error("@Contended padding is not in effect: the ring buffer's head and tail are {} bytes apart"
                        + " and so share a {}-byte cache line. Set -XX:-RestrictContended -XX:ContendedPaddingWidth={}" +
                        " for optimal performance", separation, cacheLineSize, cacheLineSize);
            }
        });
    }

    /**
     * Rejects a {@code null} element, so that a slot holding nothing means exactly that.
     *
     * <p>It is a plain branch on the offer path rather than a validation setting because the single-consumer
     * buffers rely on it: they carry no sequence array and read a slot's own emptiness as "no element here", a
     * reading a stored {@code null} would break. The branch is perfectly predicted and folds into the store
     * that follows it.
     *
     * @param element the element an offer was given
     * @throws NullPointerException if it is {@code null}
     */
    protected static void requireStorableElement(Object element) {
        if (element == null) {
            throw new NullPointerException("A ring buffer element cannot be null");
        }
    }

    protected static int calculateShiftForScale(final int scale) {
        if (4 == scale) {
            return 2;
        } else if (8 == scale) {
            return 3;
        }
        throw new IllegalArgumentException("unknown pointer size for scale=" + scale);
    }

    protected int getIndex(long i) {
        return (int) (i & mask) + bufferPadding;
    }

    private boolean isPowerOfTwo(int n) {
        return n > 0 && n != 1 && (n == Integer.highestOneBit(n));
    }

    @Override
    public int getCapacity() {
        return capacity;
    }

    @Override
    public void pollBlocking(Consumer<T> consumer, IdleStrategy idleStrategy) {
        boolean polled = poll(consumer);
        if (!polled) {
            idleStrategy.reset();
            do {
                idleStrategy.idle();
            } while (!poll(consumer));
        }
    }

    @Override
    public T pollBlocking(IdleStrategy idleStrategy) {
        T element = poll();
        if (element == null) {
            idleStrategy.reset();
            do {
                idleStrategy.idle();
            } while ((element = poll()) == null);
        }
        return element;
    }

    @Override
    public T pollBlocking(IdleStrategy idleStrategy, Duration maxWaitTime) {
        T element = poll();
        if (element == null) {
            idleStrategy.reset();
            long deadLine = System.nanoTime() + maxWaitTime.toNanos();
            do {
                idleStrategy.idle();
                if (System.nanoTime() >= deadLine) {
                    return null;
                }
            } while ((element = poll()) == null);
        }
        return element;
    }

    @Override
    public void offerBlocking(T element, IdleStrategy idleStrategy) {
        if (!offer(element)) {
            idleStrategy.reset();
            do {
                idleStrategy.idle();
            } while (!offer(element));
        }
    }

    @Override
    public <A> void offerBlocking(EventTranslatorOneArg<T, A> eventTranslator, A arg1, IdleStrategy idleStrategy) {
        if (!offer(eventTranslator, arg1)) {
            idleStrategy.reset();
            do {
                idleStrategy.idle();
            } while (!offer(eventTranslator, arg1));
        }
    }

    @Override
    public <A, B> void offerBlocking(EventTranslatorTwoArg<T, A, B> eventTranslator, A arg1, B arg2, IdleStrategy idleStrategy) {
        if (!offer(eventTranslator, arg1, arg2)) {
            idleStrategy.reset();
            do {
                idleStrategy.idle();
            } while (!offer(eventTranslator, arg1, arg2));
        }
    }

    @Override
    public <A, B> void offerBlocking(EventTranslatorThreeLongArg<T, A, B> eventTranslator, long arg1, A arg2, B arg3, IdleStrategy idleStrategy) {
        if (!offer(eventTranslator, arg1, arg2, arg3)) {
            idleStrategy.reset();
            do {
                idleStrategy.idle();
            } while (!offer(eventTranslator, arg1, arg2, arg3));
        }
    }

    @Override
    public <A, B, C> void offerBlocking(EventTranslatorThreeArg<T, A, B, C> eventTranslator, A arg1, B arg2, C arg3, IdleStrategy idleStrategy) {
        if (!offer(eventTranslator, arg1, arg2, arg3)) {
            idleStrategy.reset();
            do {
                idleStrategy.idle();
            } while (!offer(eventTranslator, arg1, arg2, arg3));
        }
    }

    @Override
    public <A, B, C, D> void offerBlocking(EventTranslatorFourArg<T, A, B, C, D> eventTranslator, A arg1, B arg2, C arg3, D arg4, IdleStrategy idleStrategy) {
        if (!offer(eventTranslator, arg1, arg2, arg3, arg4)) {
            idleStrategy.reset();
            do {
                idleStrategy.idle();
            } while (!offer(eventTranslator, arg1, arg2, arg3, arg4));
        }
    }

    @Override
    public <A, B, C, D, E> void offerBlocking(EventTranslatorFiveArg<T, A, B, C, D, E> eventTranslator, A arg1, B arg2, C arg3, D arg4, E arg5, IdleStrategy idleStrategy) {
        if (!offer(eventTranslator, arg1, arg2, arg3, arg4, arg5)) {
            idleStrategy.reset();
            do {
                idleStrategy.idle();
            } while (!offer(eventTranslator, arg1, arg2, arg3, arg4, arg5));
        }
    }

    @Override
    public void forEachEntry(Consumer<T> consumer) {
        for (T datum : buffer) {
            if (datum != null) {
                consumer.accept(datum);
            }
        }
    }

    @Override
    public int getSize() {
        return (int) (getCurrentTail() - getCurrentHead());
    }

    @Override
    public boolean isEmpty() {
        return getCurrentHead() >= getCurrentTail();
    }

    @Override
    public boolean isNotEmpty() {
        return !isEmpty();
    }

    @Override
    public boolean isFull() {
        return (getCurrentTail() - getCurrentHead()) == capacity;
    }

    protected abstract void resetTailAndHead();

}
