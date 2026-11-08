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
package org.lolaf.ringos.rb;

import org.lolaf.ringos.idling.RetryStrategy;

import java.nio.ByteBuffer;

/**
 * Variable-length frames in one off-heap buffer, written by one producer or several, as a {@link ProducerType} says,
 * and read by any number of independent {@link ByteRingReader}s.
 *
 * <p>With {@link ProducerType#MULTI}, writers take turns behind a spin lock held for the copy of one frame: a writer
 * descheduled while holding it delays the other writers, never a reader. Lock-free claiming was ruled out: a
 * descheduled writer could be lapped by the others and then write over a newer frame that readers accept.
 *
 * <p>A write never waits: when the buffer is full it overwrites the oldest frames, and a reader that falls a whole
 * capacity behind is told it was lapped. So a slow reader can never stall a producer. A producer that would rather
 * wait for some readers uses {@link #writeRetrying} with {@link #newGatingReader() gating readers}. A frame takes its
 * payload plus a 16-byte header, rounded up to 16 bytes. Neither side allocates.
 *
 * <p>Every frame has a position: the count of bytes written before it since the buffer was built. A position
 * stays valid as long as the frame is in the buffer, so a reader can resume from one.
 */
public interface BroadcastByteRingBuffer {

    /**
     * Returned by {@link #writeRetrying} when gating readers kept the room the frame needs until the retry strategy
     * gave up.
     */
    long GATED = -1;

    int getCapacity();

    /**
     * The longest payload a frame may carry, an eighth of the capacity less the header.
     */
    int getMaxPayloadLength();

    /**
     * Claims {@code length} bytes, has the translator fill them and publishes the frame.
     *
     * @param type       a value of the caller's, at least 0, handed back with the frame
     * @param length     the payload's length, at most {@link #getMaxPayloadLength()}
     * @param translator fills the payload
     * @param arg        handed to {@code translator}
     * @return the frame's position
     * @throws IllegalArgumentException if {@code type} is negative or {@code length} exceeds
     *                                  {@link #getMaxPayloadLength()}
     */
    <A> long write(int type, int length, ByteTranslator<A> translator, A arg);

    /**
     * Copies {@code length} bytes of {@code src} from {@code srcIndex}, leaving its position and limit alone.
     *
     * @param type     a value of the caller's, at least 0, handed back with the frame
     * @param src      the payload's bytes
     * @param srcIndex where the payload starts in {@code src}
     * @param length   the payload's length, at most {@link #getMaxPayloadLength()}
     * @return the frame's position
     * @throws IllegalArgumentException if {@code type} is negative or {@code length} exceeds
     *                                  {@link #getMaxPayloadLength()}
     * @throws IndexOutOfBoundsException if the range is not within {@code src}'s limit, nothing being written
     * @see #write(int, int, ByteTranslator, Object)
     */
    long write(int type, ByteBuffer src, int srcIndex, int length);

    /**
     * Writes as {@link #write(int, int, ByteTranslator, Object)} does, but never over a frame a gating reader may still
     * read. Takes the writers' lock even with {@link ProducerType#SINGLE}, which is what lets a gating reader move back
     * safely.
     *
     * @param type          a value of the caller's, at least 0, handed back with the frame
     * @param length        the payload's length, at most {@link #getMaxPayloadLength()}
     * @param translator    fills the payload once there is room, under the writers' lock
     * @param arg           handed to {@code translator}
     * @param retryStrategy how to wait while gating readers hold the room and when to give up;
     *                      {@link RetryStrategy#reset()} is called on it once, before the first retry, and never when
     *                      there is room straight away
     * @return the frame's position, or {@link #GATED} with nothing written when {@code retryStrategy} gave up
     * @throws IllegalArgumentException if {@code type} is negative or {@code length} exceeds
     *                                  {@link #getMaxPayloadLength()}
     */
    <A> long writeRetrying(int type, int length, ByteTranslator<A> translator, A arg, RetryStrategy retryStrategy);

    /**
     * Copies {@code length} bytes of {@code src} from {@code srcIndex}, leaving its position and limit alone, as
     * {@link #writeRetrying(int, int, ByteTranslator, Object, RetryStrategy)} would write them.
     *
     * @param type          a value of the caller's, at least 0, handed back with the frame
     * @param src           the payload's bytes
     * @param srcIndex      where the payload starts in {@code src}
     * @param length        the payload's length, at most {@link #getMaxPayloadLength()}
     * @param retryStrategy how to wait while gating readers hold the room and when to give up;
     *                      {@link RetryStrategy#reset()} is called on it once, before the first retry, and never when
     *                      there is room straight away
     * @return the frame's position, or {@link #GATED} with nothing written when {@code retryStrategy} gave up
     * @throws IllegalArgumentException if {@code type} is negative or {@code length} exceeds
     *                                  {@link #getMaxPayloadLength()}
     * @throws IndexOutOfBoundsException if the range is not within {@code src}'s limit, nothing being written
     */
    long writeRetrying(int type, ByteBuffer src, int srcIndex, int length, RetryStrategy retryStrategy);

    /**
     * The position the next frame will take; a snapshot that may be stale on return.
     */
    long getTailPosition();

    /**
     * A reader positioned at the tail, so it reads the frames published from now on.
     */
    ByteRingReader newReader();

    /**
     * A reader at the tail that {@link #writeRetrying} never laps until it is {@link ByteRingReader#close() closed}: the
     * frame it last read stays in the buffer for {@link ByteRingReader#unread()}. A plain {@link #write} still
     * overwrites it.
     */
    ByteRingReader newGatingReader();
}
