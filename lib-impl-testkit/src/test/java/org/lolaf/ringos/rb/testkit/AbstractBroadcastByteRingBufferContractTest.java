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
import org.lolaf.ringos.idling.RetryStrategy;
import org.lolaf.ringos.rb.BroadcastByteRingBuffer;
import org.lolaf.ringos.rb.ProducerType;
import org.lolaf.ringos.rb.ByteRingReader;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What one thread can expect of a {@link BroadcastByteRingBuffer} and its readers: positions, overwriting, being
 * lapped, resuming, and gating readers holding back the writes that retry. Run once per {@link ProducerType} mode.
 */
public abstract class AbstractBroadcastByteRingBufferContractTest extends AbstractByteRingBufferTestKit {

    private static final int CAPACITY = 1024;
    private static final int HEADER = 16;
    private static final int MAX_FRAME = CAPACITY / 8;

    private static final RetryStrategy GIVE_UP = new RetryStrategy() {
        @Override
        public void reset() {
        }

        @Override
        public boolean awaitRetry() {
            return false;
        }
    };

    private final Frame frame = new Frame();
    private final ByteBuffer dst = ByteBuffer.allocate(CAPACITY);

    protected abstract ProducerType producers();

    private BroadcastByteRingBuffer ring() {
        return byteRingBufferBuilder().buildBroadcast(CAPACITY, producers());
    }

    private long write(BroadcastByteRingBuffer ring, int sequence, int length) {
        return ring.write(sequence, length, FILL, frame.of(1, sequence));
    }

    private long writeOrGiveUp(BroadcastByteRingBuffer ring, int sequence, int length) {
        return ring.writeRetrying(sequence, length, FILL, frame.of(1, sequence), GIVE_UP);
    }

    private void fillWithLargestFrames(BroadcastByteRingBuffer ring) {
        for (int i = 0; i < 8; i++) {
            assertThat(writeOrGiveUp(ring, i, MAX_FRAME - HEADER)).isEqualTo((long) i * MAX_FRAME);
        }
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

    @Test
    void aGatingReaderKeepsRetryingWritesOffWhatItMayStillRead() {
        BroadcastByteRingBuffer ring = ring();
        ByteRingReader gating = ring.newGatingReader();
        fillWithLargestFrames(ring);

        assertThat(writeOrGiveUp(ring, 8, MAX_FRAME - HEADER)).isEqualTo(BroadcastByteRingBuffer.GATED);
        assertThat(ring.getTailPosition()).as("nothing written").isEqualTo(CAPACITY);
        assertReads(gating, 0, MAX_FRAME - HEADER, 0);
        assertThat(writeOrGiveUp(ring, 8, MAX_FRAME - HEADER)).as("the frame read is kept for unread")
                .isEqualTo(BroadcastByteRingBuffer.GATED);
        assertReads(gating, 1, MAX_FRAME - HEADER, MAX_FRAME);
        assertThat(writeOrGiveUp(ring, 8, MAX_FRAME - HEADER)).isEqualTo(CAPACITY);
        for (int i = 2; i <= 8; i++) {
            assertReads(gating, i, MAX_FRAME - HEADER, (long) i * MAX_FRAME);
        }
    }

    @Test
    void aWriteRetriesUntilTheGatingReaderMakesRoom() {
        BroadcastByteRingBuffer ring = ring();
        ByteRingReader gating = ring.newGatingReader();
        fillWithLargestFrames(ring);
        int[] resets = new int[1];
        int[] retries = new int[1];
        RetryStrategy readingOneFramePerRetry = new RetryStrategy() {
            @Override
            public void reset() {
                resets[0]++;
            }

            @Override
            public boolean awaitRetry() {
                retries[0]++;
                assertThat(gating.readInto(dst, 0)).isEqualTo(MAX_FRAME - HEADER);
                return true;
            }
        };

        long position = ring.writeRetrying(8, MAX_FRAME - HEADER, FILL, frame.of(1, 8), readingOneFramePerRetry);

        assertThat(position).isEqualTo(CAPACITY);
        assertThat(resets[0]).isEqualTo(1);
        assertThat(retries[0]).as("the second frame read makes room, the first being kept for unread").isEqualTo(2);
    }

    @Test
    void aFrameCopiedFromASourceBufferIsGatedLikeOneTranslated() {
        BroadcastByteRingBuffer ring = ring();
        ByteRingReader gating = ring.newGatingReader();
        fillWithLargestFrames(ring);
        ByteBuffer src = ByteBuffer.allocate(64);
        fill(src, 10, 40, 1, 8);
        src.position(3).limit(60);

        assertThat(ring.writeRetrying(8, src, 10, 40, GIVE_UP)).isEqualTo(BroadcastByteRingBuffer.GATED);
        assertReads(gating, 0, MAX_FRAME - HEADER, 0);
        assertReads(gating, 1, MAX_FRAME - HEADER, MAX_FRAME);
        long position = ring.writeRetrying(8, src, 10, 40, GIVE_UP);

        assertThat(position).isEqualTo(CAPACITY);
        assertThat(src.position()).isEqualTo(3);
        assertThat(src.limit()).isEqualTo(60);
        for (int i = 2; i < 8; i++) {
            assertReads(gating, i, MAX_FRAME - HEADER, (long) i * MAX_FRAME);
        }
        assertReads(gating, 8, 40, position);
    }

    @Test
    void aWriteWithRoomNeverCallsItsRetryStrategy() {
        BroadcastByteRingBuffer ring = ring();
        ring.newGatingReader();
        RetryStrategy failing = new RetryStrategy() {
            @Override
            public void reset() {
                throw new AssertionError("reset");
            }

            @Override
            public boolean awaitRetry() {
                throw new AssertionError("awaitRetry");
            }
        };

        assertThat(ring.writeRetrying(1, 8, FILL, frame.of(1, 1), failing)).isZero();
    }

    @Test
    void aReaderThatDoesNotGateHoldsNoWriteBack() {
        BroadcastByteRingBuffer ring = ring();
        ByteRingReader reader = ring.newReader();
        fillWithLargestFrames(ring);

        assertThat(writeOrGiveUp(ring, 8, MAX_FRAME - HEADER)).isEqualTo(CAPACITY);
        assertThat(reader.readInto(dst, 0)).isEqualTo(ByteRingReader.LAPPED);
    }

    @Test
    void aClosedGatingReaderNoLongerHoldsWritesBackAndCanBeLapped() {
        BroadcastByteRingBuffer ring = ring();
        ByteRingReader gating = ring.newGatingReader();
        fillWithLargestFrames(ring);

        gating.close();

        assertThat(writeOrGiveUp(ring, 8, MAX_FRAME - HEADER)).isEqualTo(CAPACITY);
        assertThat(gating.readInto(dst, 0)).isEqualTo(ByteRingReader.LAPPED);
    }

    @Test
    void aSourceThatCannotSupplyThePayloadIsRefusedAndTheRingStaysUsable() {
        BroadcastByteRingBuffer ring = ring();
        ByteRingReader reader = ring.newReader();
        ByteBuffer tooShort = ByteBuffer.allocate(64).limit(8);

        assertThatThrownBy(() -> ring.write(1, tooShort, 0, 64)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> ring.write(1, null, 0, 8)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> ring.writeRetrying(1, tooShort, 0, 64, GIVE_UP)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> ring.writeRetrying(1, null, 0, 8, GIVE_UP)).isInstanceOf(NullPointerException.class);

        assertThat(ring.getTailPosition()).isZero();
        assertThat(reader.readInto(dst, 0)).isEqualTo(ByteRingReader.NO_FRAME);
        assertReads(reader, 2, 40, write(ring, 2, 40));
    }

    @Test
    void aGatingReaderCannotUnreadAFrameAPlainWriteOverwrote() {
        BroadcastByteRingBuffer ring = ring();
        ByteRingReader gating = ring.newGatingReader();
        write(ring, 0, MAX_FRAME - HEADER);
        assertReads(gating, 0, MAX_FRAME - HEADER, 0);
        for (int i = 1; i <= 8; i++) {
            write(ring, i, MAX_FRAME - HEADER);
        }

        assertThat(gating.unread()).isFalse();
    }

    @Test
    void aPlainWriteStillOverwritesAGatingReader() {
        BroadcastByteRingBuffer ring = ring();
        ByteRingReader gating = ring.newGatingReader();
        fillWithLargestFrames(ring);

        write(ring, 8, MAX_FRAME - HEADER);

        assertThat(gating.readInto(dst, 0)).isEqualTo(ByteRingReader.LAPPED);
    }

    @Test
    void aGatingReaderSeekingBackHoldsWritesFromItsNewPosition() {
        BroadcastByteRingBuffer ring = ring();
        ByteRingReader gating = ring.newGatingReader();
        for (int i = 0; i < 3; i++) {
            writeOrGiveUp(ring, i, MAX_FRAME - HEADER);
            assertReads(gating, i, MAX_FRAME - HEADER, (long) i * MAX_FRAME);
        }

        assertThat(gating.seek(MAX_FRAME)).isTrue();

        for (int i = 3; i <= 8; i++) {
            assertThat(writeOrGiveUp(ring, i, MAX_FRAME - HEADER)).isEqualTo((long) i * MAX_FRAME);
        }
        assertThat(writeOrGiveUp(ring, 9, MAX_FRAME - HEADER)).isEqualTo(BroadcastByteRingBuffer.GATED);
        assertReads(gating, 1, MAX_FRAME - HEADER, MAX_FRAME);
    }

    @Test
    void aReaderReadsTheFrameItUnreadAgain() {
        BroadcastByteRingBuffer ring = ring();
        ByteRingReader reader = ring.newReader();
        long first = write(ring, 1, 20);
        long second = write(ring, 2, 30);
        assertReads(reader, 1, 20, first);

        assertThat(reader.unread()).isTrue();

        assertThat(reader.getPosition()).isEqualTo(first);
        assertReads(reader, 1, 20, first);
        assertReads(reader, 2, 30, second);
    }

    @Test
    void anUnreadNeedsAFrameReadSinceTheReaderLastMoved() {
        BroadcastByteRingBuffer ring = ring();
        ByteRingReader reader = ring.newReader();
        long position = write(ring, 1, 20);

        assertThat(reader.unread()).as("nothing read yet").isFalse();
        assertReads(reader, 1, 20, position);
        assertThat(reader.unread()).isTrue();
        assertThat(reader.unread()).as("only once").isFalse();
        reader.seekToTail();
        assertThat(reader.unread()).as("after a seek").isFalse();
        assertThat(reader.getPosition()).isEqualTo(ring.getTailPosition());
    }

    @Test
    void aReaderThatDoesNotGateCannotUnreadAnOverwrittenFrame() {
        BroadcastByteRingBuffer ring = ring();
        ByteRingReader reader = ring.newReader();
        write(ring, 0, MAX_FRAME - HEADER);
        assertReads(reader, 0, MAX_FRAME - HEADER, 0);
        for (int i = 1; i <= 8; i++) {
            write(ring, i, MAX_FRAME - HEADER);
        }

        assertThat(reader.unread()).isFalse();
        assertThat(reader.getPosition()).isEqualTo(MAX_FRAME);
    }
}
