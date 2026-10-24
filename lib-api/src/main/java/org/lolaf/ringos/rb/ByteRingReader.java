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
     * The position of the next frame to read.
     */
    long getPosition();

    /**
     * Copies the next frame's payload into {@code dst} at {@code dstIndex} and moves past it.
     *
     * @return the payload length, 0 when no frame was published since, or {@link #LAPPED}, after which the reader
     * stays where it is until {@link #seek} or {@link #seekToTail} moves it; what {@code dst} holds is then garbage
     * @throws IllegalArgumentException if {@code dst} has fewer than the frame's bytes after {@code dstIndex}; sizing
     *                                  it to the ring's maximum payload rules this out
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
     * Moves to a frame position, to resume where an earlier reader stopped.
     *
     * @return {@code false}, and the reader left where it was, when {@code position} is neither the tail nor the start
     * of a frame still in the buffer
     */
    boolean seek(long position);

    void seekToTail();
}
