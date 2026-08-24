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


public abstract class AbstractSpScRingBuffer<T> extends AbstractSequencedRingBuffer<T> {


    protected AbstractSpScRingBuffer(int capacity, boolean bufferPaddingEnabled) {
        this(capacity, bufferPaddingEnabled, null);
    }

    protected AbstractSpScRingBuffer(int capacity, boolean bufferPaddingEnabled, IntFunction<T> elementInstanceProducer) {
        super(capacity, bufferPaddingEnabled, elementInstanceProducer);
    }

    @Override
    public boolean offer(T item) {
        requireStorableElement(item);
        long currentTail = getCurrentTail();
        long nextTail = currentTail + 1;
        int index = getIndex(currentTail);
        if (getSequence(index) < currentTail) {
            return false;
        }

        tail = nextTail;
        setBufferElement(index, item);
        setSequence(index, nextTail);
        return true;
    }

    @Override
    public <A> boolean offer(EventTranslatorOneArg<T, A> eventTranslator, A arg1) {
        long currentTail = getCurrentTail();
        long nextTail = currentTail + 1;
        int index = getIndex(currentTail);
        if (getSequence(index) < currentTail) {
            return false;
        }

        tail = nextTail;
        eventTranslator.translate(getBufferElement(index), arg1);
        setSequence(index, nextTail);
        return true;
    }

    @Override
    public <A, B> boolean offer(EventTranslatorTwoArg<T, A, B> eventTranslator, A arg1, B arg2) {
        long currentTail = getCurrentTail();
        long nextTail = currentTail + 1;
        int index = getIndex(currentTail);
        if (getSequence(index) < currentTail) {
            return false;
        }

        tail = nextTail;
        eventTranslator.translate(getBufferElement(index), arg1, arg2);
        setSequence(index, nextTail);
        return true;
    }

    @Override
    public <A, B, C> boolean offer(EventTranslatorThreeArg<T, A, B, C> eventTranslator, A arg1, B arg2, C arg3) {
        long currentTail = getCurrentTail();
        long nextTail = currentTail + 1;
        int index = getIndex(currentTail);
        if (getSequence(index) < currentTail) {
            return false;
        }

        tail = nextTail;
        eventTranslator.translate(getBufferElement(index), arg1, arg2, arg3);
        setSequence(index, nextTail);
        return true;
    }

    @Override
    public <A, B> boolean offer(EventTranslatorThreeLongArg<T, A, B> eventTranslator, long arg1, A arg2, B arg3) {
        long currentTail = getCurrentTail();
        long nextTail = currentTail + 1;
        int index = getIndex(currentTail);
        if (getSequence(index) < currentTail) {
            return false;
        }

        tail = nextTail;
        eventTranslator.translate(getBufferElement(index), arg1, arg2, arg3);
        setSequence(index, nextTail);
        return true;
    }

    @Override
    public <A, B, C, D> boolean offer(EventTranslatorFourArg<T, A, B, C, D> eventTranslator, A arg1, B arg2, C arg3, D arg4) {
        long currentTail = getCurrentTail();
        long nextTail = currentTail + 1;
        int index = getIndex(currentTail);
        if (getSequence(index) < currentTail) {
            return false;
        }

        tail = nextTail;
        eventTranslator.translate(getBufferElement(index), arg1, arg2, arg3, arg4);
        setSequence(index, nextTail);
        return true;
    }

    @Override
    public <A, B, C, D, E> boolean offer(EventTranslatorFiveArg<T, A, B, C, D, E> eventTranslator, A arg1, B arg2, C arg3, D arg4, E arg5) {
        long currentTail = getCurrentTail();
        long nextTail = currentTail + 1;
        int index = getIndex(currentTail);
        if (getSequence(index) < currentTail) {
            return false;
        }

        tail = nextTail;
        eventTranslator.translate(getBufferElement(index), arg1, arg2, arg3, arg4, arg5);
        setSequence(index, nextTail);
        return true;
    }

    @Override
    public T poll() {
        long currentHead = getCurrentHead();
        long nextHead = currentHead + 1;
        int index = getIndex(currentHead);
        if (getSequence(index) < nextHead) {
            return null;
        }

        head = nextHead;
        T item = clearElementOnPoll ? getAndResetBufferElement(index) : getBufferElement(index);
        setSequence(index, nextHead + mask);
        return item;
    }

    @Override
    public boolean poll(Consumer<T> consumer) {
        long currentHead = getCurrentHead();
        long nextHead = currentHead + 1;
        int index = getIndex(currentHead);
        if (getSequence(index) < nextHead) {
            return false;
        }

        head = nextHead;
        T item = clearElementOnPoll ? getAndResetBufferElement(index) : getBufferElement(index);
        consumer.accept(item);
        setSequence(index, nextHead + mask);
        return true;
    }

    @Override
    public int poll(Consumer<T>[] consumers) {
        long currentHead = getCurrentHead();
        int available = getAvailableElementsToRead(consumers.length, currentHead);
        if (available == 0) {
            return 0;
        }
        head = currentHead + available;
        for (int i = 0; i < available; i++) {
            long headIndex = currentHead + i;
            int index = getIndex(headIndex);
            T item = clearElementOnPoll ? getAndResetBufferElement(index) : getBufferElement(index);
            consumers[i].accept(item);
            setSequence(index, headIndex + capacity);
        }
        return available;
    }

    @Override
    protected void resetTailAndHead() {
        tail = head = 0;
    }
}