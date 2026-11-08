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

import java.nio.ByteBuffer;

/**
 * Reads a {@link BroadcastByteRingBuffer} from its own position, independently of every other reader. One thread
 * at a time.
 *
 * <p>A writer may overwrite a frame while it is being read. So a frame is copied into a buffer of the caller's and
 * checked once copied: only a frame that was whole for the whole copy is reported, and a torn one never is.
 */
public interface ByteRingReader {

    /**
     * Returned by {@link #readInto} when writers have overwritten frames this reader had not read yet.
     */
    int LAPPED = -1;

    /**
     * Returned by {@link #readInto} when no frame was published past this reader's position; told apart from a
     * frame with an empty payload, whose length is 0.
     */
    int NO_FRAME = -2;

    /**
     * Returned by {@link #readInto} when the next frame is larger than the room left in {@code dst}: the reader stays
     * on it, so a caller filling a buffer with frames stops there and reads it into the next one.
     */
    int NO_ROOM = -3;

    /**
     * The position of the next frame to read.
     */
    long getPosition();

    /**
     * Copies the next frame's payload into {@code dst} at {@code dstIndex} and moves past it.
     *
     * @param dst      receives the payload, its position and limit left alone
     * @param dstIndex where the payload goes in {@code dst}
     * @return the payload length, {@link #NO_FRAME} when no frame was published since, {@link #NO_ROOM} when it does
     * not fit after {@code dstIndex}, or {@link #LAPPED}, after which the reader stays where it is until {@link #seek}
     * or {@link #seekToTail} moves it; what {@code dst} holds is then garbage
     */
    int readInto(ByteBuffer dst, int dstIndex);

    /**
     * The type of the frame last read by {@link #readInto}.
     */
    int getType();

    /**
     * The position of the frame last read by {@link #readInto}.
     */
    long getFramePosition();

    /**
     * Moves to a frame position, to resume where an earlier reader stopped. A gating reader moving back takes the
     * writers' lock.
     *
     * @param position a frame's position, from {@link #getFramePosition()} or {@link BroadcastByteRingBuffer#write},
     *                 or the tail
     * @return {@code false}, and the reader left where it was, when {@code position} is neither the tail nor the start
     * of a frame still in the buffer
     */
    boolean seek(long position);

    void seekToTail();

    /**
     * Moves back to the frame last read, so the next {@link #readInto} reads it again.
     *
     * @return {@code false}, and the reader left where it was, when no frame was read since the reader last moved, or
     * when it was overwritten, which for a gating reader takes a plain {@code write}
     */
    boolean unread();

    /**
     * Stops gating writers, for a reader from {@link BroadcastByteRingBuffer#newGatingReader()}; nothing for another.
     * The reader can still read, and be lapped. Any thread, unlike the other methods, so a writer's thread can drop a
     * reader that holds it back; but not a {@link ByteTranslator}, see there.
     */
    void close();
}
