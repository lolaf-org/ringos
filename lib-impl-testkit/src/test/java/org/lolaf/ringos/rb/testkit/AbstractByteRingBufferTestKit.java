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

import org.lolaf.ringos.rb.ByteRingBufferBuilder;
import org.lolaf.ringos.rb.ByteTranslator;

import java.nio.ByteBuffer;

/**
 * What every byte ring buffer test needs from the implementation under test, and the payloads they write.
 *
 * <p>A payload names its producer and its sequence number in its first 8 bytes, and every byte after them is a
 * function of the two and of its offset. So a reader recomputes the whole payload from its first 8 bytes, and a
 * frame torn by a concurrent overwrite, or delivered with another frame's bytes, never passes.
 */
public abstract class AbstractByteRingBufferTestKit {

    /**
     * The shortest payload {@link #fill} writes: producer and sequence number.
     */
    protected static final int MIN_PAYLOAD = 8;

    /**
     * Writes the payload {@link #fill} describes for the producer and sequence number of a {@link Frame}.
     */
    protected static final ByteTranslator<Frame> FILL = (buffer, index, length, frame) -> fill(buffer, index, length, frame.producer, frame.sequence);

    /**
     * @return the builder of the implementation under test, e.g. {@code new MethodHandleByteRbBuilder()}
     */
    protected abstract ByteRingBufferBuilder byteRingBufferBuilder();

    /**
     * Writes nothing into a payload shorter than {@link #MIN_PAYLOAD}: never past {@code length}, where the next
     * frame's header lies.
     */
    protected static void fill(ByteBuffer buffer, int index, int length, int producer, int sequence) {
        if (length < MIN_PAYLOAD) {
            return;
        }
        buffer.putInt(index, producer);
        buffer.putInt(index + 4, sequence);
        for (int i = MIN_PAYLOAD; i < length; i++) {
            buffer.put(index + i, filler(producer, sequence, i));
        }
    }

    /**
     * @return whether the payload is exactly what {@link #fill} wrote for the producer and sequence number it names
     */
    protected static boolean intact(ByteBuffer buffer, int index, int length) {
        if (length < MIN_PAYLOAD) {
            return false;
        }
        int producer = buffer.getInt(index);
        int sequence = buffer.getInt(index + 4);
        for (int i = MIN_PAYLOAD; i < length; i++) {
            if (buffer.get(index + i) != filler(producer, sequence, i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * A payload length varying with the sequence number, so frames of many sizes meet at the wrap.
     */
    protected static int payloadLength(int producer, int sequence, int maxPayloadLength) {
        return MIN_PAYLOAD + Math.floorMod(sequence * 37 + producer * 11, maxPayloadLength - MIN_PAYLOAD + 1);
    }

    private static byte filler(int producer, int sequence, int offset) {
        return (byte) (producer * 31 + sequence * 17 + offset);
    }

    /**
     * The mutable argument a producer passes {@link #FILL}, one per producer thread.
     */
    protected static final class Frame {
        int producer;
        int sequence;

        Frame of(int producer, int sequence) {
            this.producer = producer;
            this.sequence = sequence;
            return this;
        }
    }
}
