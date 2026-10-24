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
 * Variable-length frames in one off-heap buffer, written by any number of producers and read by any number of
 * independent {@link ByteRingReader}s.
 *
 * <p>A write never waits: when the buffer is full it overwrites the oldest frames, and a reader that falls a whole
 * capacity behind is told it was lapped. So a slow reader can never stall a producer. A frame takes its payload
 * plus a 16-byte header, rounded up to 8 bytes. Neither side allocates.
 *
 * <p>Every frame has a position: the count of bytes written before it since the buffer was built. A position
 * stays valid as long as the frame is in the buffer, so a reader can resume from one.
 */
public interface BroadcastByteRingBuffer {

    int getCapacity();

    /**
     * The longest payload a frame may carry, an eighth of the capacity less the header.
     */
    int getMaxPayloadLength();

    /**
     * Claims {@code length} bytes, has the translator fill them and publishes the frame.
     *
     * @param type a value of the caller's, at least 0, handed back with the frame
     * @return the frame's position
     * @throws IllegalArgumentException if {@code type} is negative or {@code length} exceeds
     *                                  {@link #getMaxPayloadLength()}
     */
    <A> long write(int type, int length, ByteTranslator<A> translator, A arg);

    /**
     * Copies {@code length} bytes of {@code src} from {@code srcIndex}, leaving its position and limit alone.
     *
     * @see #write(int, int, ByteTranslator, Object)
     */
    long write(int type, ByteBuffer src, int srcIndex, int length);

    /**
     * The position the next frame will take; a snapshot that may be stale on return.
     */
    long getTailPosition();

    /**
     * A reader positioned at the tail, so it reads the frames published from now on.
     */
    ByteRingReader newReader();
}
