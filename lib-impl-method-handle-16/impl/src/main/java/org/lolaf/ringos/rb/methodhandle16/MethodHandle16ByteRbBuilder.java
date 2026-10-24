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
package org.lolaf.ringos.rb.methodhandle16;

import org.lolaf.ringos.rb.BroadcastByteRingBuffer;
import org.lolaf.ringos.rb.ByteRingBuffer;
import org.lolaf.ringos.rb.ByteRingBufferBuilder;

public class MethodHandle16ByteRbBuilder implements ByteRingBufferBuilder {

    @Override
    public ByteRingBuffer buildQueue(int capacity) {
        return new MethodHandle16MpScByteRingBuffer(capacity);
    }

    @Override
    public BroadcastByteRingBuffer buildBroadcast(int capacity, BroadcastByteRingBuffer.Producers producers) {
        return producers == BroadcastByteRingBuffer.Producers.SINGLE
                ? new MethodHandle16SpBroadcastByteRingBuffer(capacity)
                : new MethodHandle16MpBroadcastByteRingBuffer(capacity);
    }
}
