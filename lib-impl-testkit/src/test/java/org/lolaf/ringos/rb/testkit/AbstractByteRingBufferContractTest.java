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
import org.lolaf.ringos.rb.ByteFrameHandler;
import org.lolaf.ringos.rb.ByteRingBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What one thread can expect of a {@link ByteRingBuffer}: the queue mode, where the consumer frees space and a full
 * buffer refuses a write.
 */
public abstract class AbstractByteRingBufferContractTest extends AbstractByteRingBufferTestKit {

    private static final int CAPACITY = 1024;
    private static final int MAX_FRAME = CAPACITY / 8;

    private final Frame frame = new Frame();
    private final List<String> read = new ArrayList<>();

    private ByteRingBuffer queue() {
        return byteRingBufferBuilder().buildQueue(CAPACITY);
    }

    private boolean write(ByteRingBuffer queue, int sequence, int length) {
        return queue.write(sequence, length, FILL, frame.of(1, sequence));
    }

    private int readAll(ByteRingBuffer queue) {
        return queue.read((type, buffer, index, length) -> {
            assertThat(index % 8).as("payload alignment").isZero();
            read.add(type + ":" + length + ":" + (length == 0 || intact(buffer, index, length)));
        }, Integer.MAX_VALUE);
    }

    @Test
    void theMaximumPayloadIsAnEighthOfTheCapacityLessTheHeader() {
        assertThat(queue().getCapacity()).isEqualTo(CAPACITY);
        assertThat(queue().getMaxPayloadLength()).isEqualTo(MAX_FRAME - 8);
    }

    @Test
    void aFrameIsReadBackWithItsTypeAndPayload() {
        ByteRingBuffer queue = queue();

        assertThat(write(queue, 7, 40)).isTrue();

        assertThat(readAll(queue)).isEqualTo(1);
        assertThat(read).containsExactly("7:40:true");
    }

    @Test
    void anEmptyQueueReadsNothing() {
        assertThat(queue().read((type, buffer, index, length) -> read.add("unexpected"), 10)).isZero();
        assertThat(read).isEmpty();
    }

    @Test
    void anEmptyPayloadIsAFrame() {
        ByteRingBuffer queue = queue();

        assertThat(queue.write(3, 0, FILL, frame.of(1, 3))).isTrue();

        assertThat(readAll(queue)).isEqualTo(1);
        assertThat(read).containsExactly("3:0:true");
    }

    @Test
    void framesAreReadInTheOrderTheyWereWrittenAndNoMoreThanTheLimit() {
        ByteRingBuffer queue = queue();
        for (int i = 0; i < 5; i++) {
            write(queue, i, MIN_PAYLOAD + i);
        }

        int first = queue.read((type, buffer, index, length) -> read.add(String.valueOf(type)), 2);
        int rest = queue.read((type, buffer, index, length) -> read.add(String.valueOf(type)), 10);

        assertThat(first).isEqualTo(2);
        assertThat(rest).isEqualTo(3);
        assertThat(read).containsExactly("0", "1", "2", "3", "4");
    }

    @Test
    void theWholeCapacityHoldsFramesAndAFullQueueRefusesTheNextOne() {
        ByteRingBuffer queue = queue();
        for (int i = 0; i < 8; i++) {
            assertThat(write(queue, i, MAX_FRAME - 8)).as("frame %d", i).isTrue();
        }

        assertThat(write(queue, 8, 0)).isFalse();
        assertThat(queue.getSize()).isEqualTo(CAPACITY);
        assertThat(readAll(queue)).isEqualTo(8);
        assertThat(read).allMatch(line -> line.endsWith(":true"));
        assertThat(queue.getSize()).isZero();
    }

    @Test
    void readingFreesTheSpaceForMoreFrames() {
        ByteRingBuffer queue = queue();
        for (int round = 0; round < 20; round++) {
            for (int i = 0; i < 8; i++) {
                assertThat(write(queue, round * 8 + i, MAX_FRAME - 8)).isTrue();
            }
            assertThat(readAll(queue)).isEqualTo(8);
        }

        assertThat(read).hasSize(160).allMatch(line -> line.endsWith(":true"));
    }

    @Test
    void aFrameThatDoesNotFitBeforeTheEndIsWrittenAtTheStartBehindPadding() {
        ByteRingBuffer queue = queue();
        int payload = 96;
        for (int i = 0; i < 9; i++) {
            assertThat(write(queue, i, payload)).isTrue();
        }
        assertThat(readAll(queue)).isEqualTo(9);

        assertThat(write(queue, 9, payload)).isTrue();

        assertThat(readAll(queue)).as("padding is skipped, not delivered").isEqualTo(1);
        assertThat(read).hasSize(10).allMatch(line -> line.endsWith(":96:true"));
        assertThat(queue.getSize()).isZero();
    }

    @Test
    void framesOfEverySizeSurviveManyWraps() {
        ByteRingBuffer queue = queue();
        int written = 0;
        for (int sequence = 0; sequence < 5_000; sequence++) {
            int length = payloadLength(1, sequence, queue.getMaxPayloadLength());
            while (!write(queue, sequence, length)) {
                readAll(queue);
            }
            written++;
        }
        readAll(queue);

        assertThat(read).hasSize(written).allMatch(line -> line.endsWith(":true"));
        for (int sequence = 0; sequence < written; sequence++) {
            assertThat(read.get(sequence)).startsWith(sequence + ":");
        }
    }

    @Test
    void aFrameIsCopiedFromASourceBufferLeftAsItWas() {
        ByteRingBuffer queue = queue();
        ByteBuffer src = ByteBuffer.allocate(64);
        fill(src, 10, 40, 1, 2);
        src.position(3).limit(60);

        assertThat(queue.write(2, src, 10, 40)).isTrue();

        assertThat(src.position()).isEqualTo(3);
        assertThat(src.limit()).isEqualTo(60);
        readAll(queue);
        assertThat(read).containsExactly("2:40:true");
    }

    @Test
    void aSourceThatCannotSupplyThePayloadIsRefusedAndTheQueueStaysUsable() {
        ByteRingBuffer queue = queue();
        ByteBuffer tooShort = ByteBuffer.allocate(64).limit(8);

        assertThatThrownBy(() -> queue.write(1, tooShort, 0, 64)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> queue.write(1, tooShort, -1, 8)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> queue.write(1, null, 0, 8)).isInstanceOf(NullPointerException.class);

        assertThat(queue.getSize()).isZero();
        assertThat(write(queue, 2, 40)).isTrue();
        assertThat(readAll(queue)).isEqualTo(1);
        assertThat(read).containsExactly("2:40:true");
    }

    @Test
    void aFrameWhoseHandlerThrowsIsHandedOverAgainAndThoseBeforeItAreFreed() {
        ByteRingBuffer queue = queue();
        write(queue, 0, MIN_PAYLOAD);
        write(queue, 1, MIN_PAYLOAD);
        boolean[] failing = {true};
        ByteFrameHandler handler = (type, buffer, index, length) -> {
            if (type == 1 && failing[0]) {
                throw new IllegalStateException("refused");
            }
            read.add(String.valueOf(type));
        };

        assertThatThrownBy(() -> queue.read(handler, 10)).isInstanceOf(IllegalStateException.class);
        assertThat(queue.getSize()).as("only the second frame is left").isEqualTo(16);

        failing[0] = false;
        assertThat(queue.read(handler, 10)).isEqualTo(1);
        assertThat(read).containsExactly("0", "1");
        assertThat(queue.getSize()).isZero();
    }

    @Test
    void aNegativeTypeIsRefused() {
        assertThatThrownBy(() -> queue().write(-1, 8, FILL, frame.of(1, 1))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aPayloadLongerThanTheMaximumIsRefused() {
        ByteRingBuffer queue = queue();

        assertThatThrownBy(() -> queue.write(1, queue.getMaxPayloadLength() + 1, FILL, frame.of(1, 1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> queue.write(1, -1, FILL, frame.of(1, 1))).isInstanceOf(IllegalArgumentException.class);
        assertThat(queue.getSize()).isZero();
    }
}
