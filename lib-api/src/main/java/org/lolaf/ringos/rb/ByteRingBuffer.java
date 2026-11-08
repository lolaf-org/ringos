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
 * A queue of variable-length frames in one off-heap buffer: any number of producers, one consumer.
 *
 * <p>A frame takes its payload plus an 8-byte header, rounded up to 8 bytes. Space is reclaimed as the consumer
 * reads, and a full buffer refuses a write rather than waiting. Neither side allocates.
 */
public interface ByteRingBuffer {

    /**
     * Size of the buffer in bytes, frames and headers included.
     */
    int getCapacity();

    /**
     * The longest payload a frame may carry, an eighth of the capacity less the header.
     */
    int getMaxPayloadLength();

    /**
     * Claims {@code length} bytes, has the translator fill them and publishes the frame.
     *
     * @param type    a value of the caller's, at least 0, handed back with the frame
     * @return {@code false}, and nothing written, when the buffer has no room for the frame
     * @throws IllegalArgumentException if {@code type} is negative or {@code length} exceeds
     *                                  {@link #getMaxPayloadLength()}
     */
    <A> boolean write(int type, int length, ByteTranslator<A> translator, A arg);

    /**
     * Copies {@code length} bytes of {@code src} from {@code srcIndex}, leaving its position and limit alone.
     *
     * @throws IndexOutOfBoundsException if the range is not within {@code src}'s limit, nothing being written
     * @see #write(int, int, ByteTranslator, Object)
     */
    boolean write(int type, ByteBuffer src, int srcIndex, int length);

    /**
     * Hands up to {@code limit} frames to the handler in the order they were published, then frees their space.
     * Consumer thread only. A handler's exception propagates: the frames before it are freed, and the next read starts
     * again at the frame that failed, so a handler that always fails on a frame stops the queue there.
     *
     * @return the number of frames read, 0 when none was published
     */
    int read(ByteFrameHandler handler, int limit);

    /**
     * Bytes held by published and claimed frames, headers and padding included; a snapshot that may be stale on
     * return.
     */
    int getSize();
}
