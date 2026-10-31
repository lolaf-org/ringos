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
import org.lolaf.ringos.rb.BroadcastByteRingBuffer;
import org.lolaf.ringos.rb.ProducerType;
import org.lolaf.ringos.rb.ByteRingReader;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What one thread can expect of a {@link BroadcastByteRingBuffer} and its readers: positions, overwriting, being
 * lapped and resuming. Run once per {@link ProducerType} mode.
 */
public abstract class AbstractBroadcastByteRingBufferContractTest extends AbstractByteRingBufferTestKit {

    private static final int CAPACITY = 1024;
    private static final int HEADER = 16;
    private static final int MAX_FRAME = CAPACITY / 8;

    private final Frame frame = new Frame();
    private final ByteBuffer dst = ByteBuffer.allocate(CAPACITY);

    protected abstract ProducerType producers();

    private BroadcastByteRingBuffer ring() {
        return byteRingBufferBuilder().buildBroadcast(CAPACITY, producers());
    }

    private long write(BroadcastByteRingBuffer ring, int sequence, int length) {
        return ring.write(sequence, length, FILL, frame.of(1, sequence));
    }

    private void assertReads(ByteRingReader reader, int sequence, int length, long position) {
        assertThat(reader.readInto(dst, 0)).isEqualTo(length);
        assertThat(reader.getType()).isEqualTo(sequence);
        assertThat(reader.getFramePosition()).isEqualTo(position);
        assertThat(intact(dst, 0, length)).isTrue();
        assertThat(dst.getInt(4)).isEqualTo(sequence);
    }

    @Test
    void theMaximumPayloadIsAnEighthOfTheCapacityLessTheHeader() {
        assertThat(ring().getMaxPayloadLength()).isEqualTo(MAX_FRAME - HEADER);
    }

    @Test
    void positionsCountTheBytesWrittenFramesBeingAlignedTo16() {
        BroadcastByteRingBuffer ring = ring();

        assertThat(ring.getTailPosition()).isZero();
        assertThat(write(ring, 0, 8)).isZero();
        assertThat(write(ring, 1, 17)).isEqualTo(32);
        assertThat(write(ring, 2, 0)).isEqualTo(80);
        assertThat(ring.getTailPosition()).isEqualTo(96);
    }

    @Test
    void aNewReaderReadsOnlyWhatIsWrittenAfterIt() {
        BroadcastByteRingBuffer ring = ring();
        write(ring, 0, 8);
        ByteRingReader reader = ring.newReader();
        long position = write(ring, 1, 40);

        assertReads(reader, 1, 40, position);
        assertThat(reader.readInto(dst, 0)).isEqualTo(ByteRingReader.NO_FRAME);
        assertThat(reader.getPosition()).isEqualTo(ring.getTailPosition());
    }

    @Test
    void anEmptyPayloadIsToldApartFromNoFrame() {
        BroadcastByteRingBuffer ring = ring();
        ByteRingReader reader = ring.newReader();
        ring.write(5, 0, FILL, frame.of(1, 5));

        assertThat(reader.readInto(dst, 0)).isZero();
        assertThat(reader.getType()).isEqualTo(5);
        assertThat(reader.readInto(dst, 0)).isEqualTo(ByteRingReader.NO_FRAME);
    }

    @Test
    void readersAreIndependent() {
        BroadcastByteRingBuffer ring = ring();
        ByteRingReader first = ring.newReader();
        ByteRingReader second = ring.newReader();
        long a = write(ring, 1, 20);
        long b = write(ring, 2, 30);

        assertReads(first, 1, 20, a);
        assertReads(first, 2, 30, b);
        assertReads(second, 1, 20, a);
        assertReads(second, 2, 30, b);
    }

    @Test
    void aFrameIsCopiedAtTheIndexAsked() {
        BroadcastByteRingBuffer ring = ring();
        ByteRingReader reader = ring.newReader();
        write(ring, 1, 24);

        assertThat(reader.readInto(dst, 100)).isEqualTo(24);
        assertThat(intact(dst, 100, 24)).isTrue();
    }

    @Test
    void aFrameLargerThanTheRoomLeftIsNotReadAndTheReaderStays() {
        BroadcastByteRingBuffer ring = ring();
        ByteRingReader reader = ring.newReader();
        long position = write(ring, 1, 40);

        assertThat(reader.readInto(ByteBuffer.allocate(139), 100)).isEqualTo(ByteRingReader.NO_ROOM);
        assertThat(reader.getPosition()).isEqualTo(position);
        assertReads(reader, 1, 40, position);
    }

    @Test
    void framesFillABufferUntilTheNextOneDoesNotFit() {
        BroadcastByteRingBuffer ring = ring();
        ByteRingReader reader = ring.newReader();
        write(ring, 1, 30);
        write(ring, 2, 30);
        long third = write(ring, 3, 30);
        ByteBuffer batch = ByteBuffer.allocate(70);

        assertThat(reader.readInto(batch, 0)).isEqualTo(30);
        assertThat(reader.readInto(batch, 30)).isEqualTo(30);
        assertThat(reader.readInto(batch, 60)).isEqualTo(ByteRingReader.NO_ROOM);
        assertThat(intact(batch, 0, 30) && intact(batch, 30, 30)).isTrue();
        assertReads(reader, 3, 30, third);
    }

    @Test
    void writingNeverWaitsAndAReaderLeftBehindAWholeCapacityIsLapped() {
        BroadcastByteRingBuffer ring = ring();
        ByteRingReader reader = ring.newReader();
        for (int i = 0; i < 9; i++) {
            write(ring, i, MAX_FRAME - HEADER);
        }

        assertThat(reader.readInto(dst, 0)).isEqualTo(ByteRingReader.LAPPED);
        assertThat(reader.getPosition()).as("stays until moved").isZero();
        assertThat(reader.readInto(dst, 0)).isEqualTo(ByteRingReader.LAPPED);

        reader.seekToTail();
        long position = write(ring, 9, 8);
        assertReads(reader, 9, 8, position);
    }

    @Test
    void aReaderExactlyOneCapacityBehindIsNotLapped() {
        BroadcastByteRingBuffer ring = ring();
        ByteRingReader reader = ring.newReader();
        for (int i = 0; i < 8; i++) {
            write(ring, i, MAX_FRAME - HEADER);
        }

        for (int i = 0; i < 8; i++) {
            assertReads(reader, i, MAX_FRAME - HEADER, (long) i * MAX_FRAME);
        }
    }

    @Test
    void aFrameThatDoesNotFitBeforeTheEndIsWrittenAtTheStartBehindPadding() {
        BroadcastByteRingBuffer ring = ring();
        ByteRingReader reader = ring.newReader();
        for (int i = 0; i < 10; i++) {
            write(ring, i, 80);
        }
        for (int i = 0; i < 10; i++) {
            assertReads(reader, i, 80, i * 96L);
        }

        long position = write(ring, 10, 80);

        assertThat(position).as("past the padding to the end").isEqualTo(CAPACITY);
        assertReads(reader, 10, 80, position);
    }

    @Test
    void framesOfEverySizeSurviveManyWrapsForAReaderKeepingUp() {
        BroadcastByteRingBuffer ring = ring();
        ByteRingReader reader = ring.newReader();
        for (int sequence = 0; sequence < 5_000; sequence++) {
            int length = payloadLength(1, sequence, ring.getMaxPayloadLength());
            long position = write(ring, sequence, length);
            assertReads(reader, sequence, length, position);
        }
    }

    @Test
    void aReaderResumesFromTheStartOfAFrameStillInTheBuffer() {
        BroadcastByteRingBuffer ring = ring();
        write(ring, 0, 8);
        long second = write(ring, 1, 40);
        write(ring, 2, 8);
        ByteRingReader reader = ring.newReader();

        assertThat(reader.seek(second)).isTrue();

        assertReads(reader, 1, 40, second);
    }

    @Test
    void aReaderCanSeekToTheTail() {
        BroadcastByteRingBuffer ring = ring();
        write(ring, 0, 8);
        ByteRingReader reader = ring.newReader();

        assertThat(reader.seek(ring.getTailPosition())).isTrue();
        assertThat(reader.readInto(dst, 0)).isEqualTo(ByteRingReader.NO_FRAME);
    }

    @Test
    void aSeekToAPositionThatIsNotAFrameStillInTheBufferIsRefused() {
        BroadcastByteRingBuffer ring = ring();
        long first = write(ring, 0, MAX_FRAME - HEADER);
        long second = write(ring, 1, 8);
        ByteRingReader reader = ring.newReader();
        long tail = ring.getTailPosition();

        assertThat(reader.seek(second + 16)).as("inside a frame").isFalse();
        assertThat(reader.seek(second + 1)).as("unaligned").isFalse();
        assertThat(reader.seek(tail + 16)).as("past the tail").isFalse();
        for (int i = 0; i < 8; i++) {
            write(ring, 2 + i, MAX_FRAME - HEADER);
        }
        assertThat(reader.seek(first)).as("overwritten").isFalse();
        assertThat(reader.getPosition()).isEqualTo(tail);
    }

    @Test
    void aFrameIsCopiedFromASourceBufferLeftAsItWas() {
        BroadcastByteRingBuffer ring = ring();
        ByteRingReader reader = ring.newReader();
        ByteBuffer src = ByteBuffer.allocate(64);
        fill(src, 10, 40, 1, 2);
        src.position(3).limit(60);

        long position = ring.write(2, src, 10, 40);

        assertThat(src.position()).isEqualTo(3);
        assertThat(src.limit()).isEqualTo(60);
        assertReads(reader, 2, 40, position);
    }

    @Test
    void aNegativeTypeOrAnOversizedPayloadIsRefused() {
        BroadcastByteRingBuffer ring = ring();

        assertThatThrownBy(() -> ring.write(-1, 8, FILL, frame.of(1, 1))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ring.write(1, ring.getMaxPayloadLength() + 1, FILL, frame.of(1, 1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(ring.getTailPosition()).isZero();
    }
}
