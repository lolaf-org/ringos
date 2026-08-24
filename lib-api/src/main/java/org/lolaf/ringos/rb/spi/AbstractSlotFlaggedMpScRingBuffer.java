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

import java.util.function.Consumer;

/**
 * A multi-producer, single-consumer ring buffer that publishes through the slot itself rather than through a
 * sequence number beside it.
 *
 * <p>A slot holding {@code null} is free and a slot holding anything else carries an element, which works only
 * because {@link RingBuffer#offer(Object)} refuses a {@code null} element. A producer claims a slot by advancing
 * {@code tail}, then releases its element into it; the consumer acquires the slot, empties it, and only then
 * releases the advanced {@code head}. Emptying before publishing the new head is what stops a producer from
 * writing into a slot the consumer has read but not yet cleared.
 *
 * <p>The point of it is one cache line instead of two. {@link AbstractSequencedRingBuffer} moves both the slot
 * and its sequence between the producer's core and the consumer's for every element, each written by both sides;
 * here only the slot travels. What pays for that is generality — the design cannot arbitrate between several
 * consumers, and it cannot host pooled element instances, since a slot pre-filled with an instance is never
 * empty and so can never say "nothing here". Buffers built with an element instance producer therefore keep the
 * sequenced implementation, and the {@code EventTranslator} overloads, which exist to populate a pooled instance
 * in place, are unsupported here.
 *
 * <p>Producers avoid reading the consumer's {@code head} on every offer by keeping {@link #sharedHeadCache}, a
 * lower bound on it. It is consulted first and only refreshed from {@code head} when it says the buffer is full,
 * so the consumer's cache line is touched on the rare offer rather than the common one.
 */
public abstract class AbstractSlotFlaggedMpScRingBuffer<T> extends AbstractRingBuffer<T> {

    /**
     * A lower bound on {@code head}, shared by the producers so that they rarely read the real one. Stale by
     * construction: it only ever lags, so acting on it can report a full buffer that has since drained, which
     * the refresh in {@link #offer(Object)} then corrects.
     */
    @Contended
    private volatile long sharedHeadCache;

    protected AbstractSlotFlaggedMpScRingBuffer(int capacity, boolean bufferPaddingEnabled) {
        super(capacity, bufferPaddingEnabled, null);
    }

    /**
     * Reads a slot with acquire semantics, so that an element found there carries with it everything the
     * producer wrote before releasing it.
     */
    protected abstract T getBufferElementAcquire(int index);

    /**
     * Publishes an element into a slot with release semantics, which is the store that hands it to the consumer.
     */
    protected abstract void setBufferElementRelease(int index, T value);

    protected abstract void clearBufferElementPlain(int index);

    protected abstract boolean tailCompareAndSwap(long current, long next);

    /**
     * Publishes an advanced {@code head} with release semantics, so a producer that sees it also sees the slot
     * the consumer emptied beforehand.
     */
    protected abstract void setHeadRelease(long value);

    @Override
    public boolean offer(T item) {
        requireStorableElement(item);

        long currentHead = sharedHeadCache;
        long bufferLimit = currentHead + capacity;
        long currentTail;
        do {
            currentTail = getCurrentTail();
            if (currentTail >= bufferLimit) {
                currentHead = getCurrentHead();
                bufferLimit = currentHead + capacity;
                if (currentTail >= bufferLimit) {
                    return false;
                }
                sharedHeadCache = currentHead;
            }
        } while (!tailCompareAndSwap(currentTail, currentTail + 1));

        setBufferElementRelease(getIndex(currentTail), item);
        return true;
    }

    @Override
    public T poll() {
        long currentHead = head;
        int index = getIndex(currentHead);
        T item = getBufferElementAcquire(index);
        if (item == null) {
            return null;
        }

        clearBufferElementPlain(index);
        setHeadRelease(currentHead + 1);
        return item;
    }

    @Override
    public boolean poll(Consumer<T> consumer) {
        long currentHead = head;
        int index = getIndex(currentHead);
        T item = getBufferElementAcquire(index);
        if (item == null) {
            return false;
        }

        consumer.accept(item);
        clearBufferElementPlain(index);
        setHeadRelease(currentHead + 1);
        return true;
    }

    @Override
    public int poll(Consumer<T>[] consumers) {
        long currentHead = head;
        int polled = 0;
        while (polled < consumers.length) {
            int index = getIndex(currentHead + polled);
            T item = getBufferElementAcquire(index);
            if (item == null) {
                break;
            }
            consumers[polled].accept(item);
            clearBufferElementPlain(index);
            polled++;
        }
        if (polled > 0) {
            setHeadRelease(currentHead + polled);
        }
        return polled;
    }

    @Override
    public T peek() {
        return getBufferElementAcquire(getIndex(head));
    }

    @Override
    public void forEach(Consumer<T> consumer) {
        long currentTail = getCurrentTail();
        for (long pos = getCurrentHead(); pos < currentTail; pos++) {
            T item = getBufferElementAcquire(getIndex(pos));
            if (item != null) {
                consumer.accept(item);
            }
        }
    }

    @Override
    public void clear() {
        resetTailAndHead();
        for (int i = 0; i < capacity; i++) {
            clearBufferElementPlain(i + bufferPadding);
        }
        sharedHeadCache = 0;
    }

    @Override
    public <A> boolean offer(EventTranslatorOneArg<T, A> eventTranslator, A arg1) {
        throw translatorsUnsupported();
    }

    @Override
    public <A, B> boolean offer(EventTranslatorTwoArg<T, A, B> eventTranslator, A arg1, B arg2) {
        throw translatorsUnsupported();
    }

    @Override
    public <A, B, C> boolean offer(EventTranslatorThreeArg<T, A, B, C> eventTranslator, A arg1, B arg2, C arg3) {
        throw translatorsUnsupported();
    }

    @Override
    public <A, B> boolean offer(EventTranslatorThreeLongArg<T, A, B> eventTranslator, long arg1, A arg2, B arg3) {
        throw translatorsUnsupported();
    }

    @Override
    public <A, B, C, D> boolean offer(EventTranslatorFourArg<T, A, B, C, D> eventTranslator, A arg1, B arg2, C arg3, D arg4) {
        throw translatorsUnsupported();
    }

    @Override
    public <A, B, C, D, E> boolean offer(EventTranslatorFiveArg<T, A, B, C, D, E> eventTranslator, A arg1, B arg2, C arg3, D arg4, E arg5) {
        throw translatorsUnsupported();
    }

    private UnsupportedOperationException translatorsUnsupported() {
        return new UnsupportedOperationException("This buffer holds no pooled element instances for a translator"
                + " to populate, because it reads a slot's own emptiness as the absence of an element. Build it"
                + " with an element instance producer to get the sequenced implementation, which supports them.");
    }
}
