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

import java.util.function.Consumer;
import java.util.function.IntFunction;

/**
 * A ring buffer that publishes through a sequence number held per slot, alongside the slot itself.
 *
 * <p>A producer writes its element and then releases the slot's sequence; a consumer acquires that sequence and
 * only then reads the element. The sequence is what carries the happens-before edge, which is why the element
 * accesses either side of it can be plain. It is also what lets a producer decide the buffer is full without
 * reading the consumer's {@code head}, and what lets several consumers agree on who took which slot.
 *
 * <p>That generality is not free: the sequence lives in an array of its own, so every element handed from a
 * producer to a consumer moves two cache lines between their cores rather than one — the slot and its sequence,
 * each written by both sides. A buffer with a single consumer and no pooled elements can carry the same
 * information in the slot's own emptiness and pay for one line; a buffer whose slots hold pooled instances
 * cannot, because such a slot is never empty, and so it belongs here.
 */
public abstract class AbstractSequencedRingBuffer<T> extends AbstractRingBuffer<T> {

    protected final long[] sequences;

    protected AbstractSequencedRingBuffer(int capacity, boolean bufferPaddingEnabled, IntFunction<T> elementInstanceProducer) {
        super(capacity, bufferPaddingEnabled, elementInstanceProducer);
        this.sequences = new long[capacity + (2 * bufferPadding)];
        initSequence();
    }

    private void initSequence() {
        for (int i = 0; i < getCapacity(); i++) {
            setSequence(i + bufferPadding, i);
        }
    }


    protected abstract T getBufferElement(int index);

    protected abstract T getAndResetBufferElement(int index);

    protected abstract void setBufferElement(int index, T value);

    protected abstract long getSequence(int index);

    protected abstract void setSequence(int index, long value);

    protected int getAvailableElementsToRead(int batchSize, long currentHead) {
        int available = 0;
        for (int i = 0; i < batchSize; i++) {
            long nextHead = currentHead + i;
            int index = getIndex(nextHead);
            if (getSequence(index) > nextHead) {
                available++;
            } else {
                break;
            }
        }
        return available;
    }


    @Override
    public T peek() {
        long currentHead = getCurrentHead();
        long nextHead = currentHead + 1;
        int index = getIndex(currentHead);
        if (getSequence(index) < nextHead) {
            return null;
        }
        return getBufferElement(index);
    }


    @Override
    public void forEach(Consumer<T> consumer) {
        long currentHead = getCurrentHead();
        long currentTail = getCurrentTail();
        for (long pos = currentHead; pos < currentTail; pos++) {
            int index = getIndex(pos);
            if (getSequence(index) - pos == 1) {
                consumer.accept(getBufferElement(index));
            }
        }
    }


    @Override
    public void clear() {
        resetTailAndHead();
        initSequence();
    }
}
