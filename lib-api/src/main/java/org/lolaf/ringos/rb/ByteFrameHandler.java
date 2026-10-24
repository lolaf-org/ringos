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
 * Receives the frames a {@link ByteRingBuffer} consumer reads.
 */
@FunctionalInterface
public interface ByteFrameHandler {

    /**
     * The payload is {@code length} bytes at {@code index} of the ring's own buffer, valid only during the call: its
     * space is reused once the call returns. Read it with absolute gets.
     */
    void onFrame(int type, ByteBuffer buffer, int index, int length);
}
