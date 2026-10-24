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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * With every implementation on the classpath, {@link ByteRingBufferFactory} finds a byte ring buffer builder on any
 * runtime, flags or not: the VarHandle one is the only one so far.
 */
class ByteRingBufferImplAllTest {

    @Test
    void theFactoryBuildsAWorkingQueue() {
        ByteRingBuffer queue = ByteRingBufferFactory.queue(1024);
        List<Integer> types = new ArrayList<>();

        assertThat(queue.write(7, 16, (buffer, index, length, arg) -> buffer.putLong(index, arg), 42L)).isTrue();
        queue.read((type, buffer, index, length) -> {
            types.add(type);
            assertThat(buffer.getLong(index)).isEqualTo(42L);
        }, 1);

        assertThat(types).containsExactly(7);
        assertThat(queue.getClass().getSimpleName()).isEqualTo("MethodHandleMpScByteRingBuffer");
    }

    @Test
    void theFactoryBuildsBothBroadcastModes() {
        BroadcastByteRingBuffer single = ByteRingBufferFactory.broadcast(1024, BroadcastByteRingBuffer.Producers.SINGLE);
        BroadcastByteRingBuffer multi = ByteRingBufferFactory.broadcast(1024, BroadcastByteRingBuffer.Producers.MULTI);
        ByteRingReader reader = single.newReader();

        long position = single.write(3, 8, (buffer, index, length, arg) -> buffer.putLong(index, arg), 42L);

        assertThat(reader.readInto(ByteBuffer.allocate(8), 0)).isEqualTo(8);
        assertThat(reader.getFramePosition()).isEqualTo(position);
        assertThat(single.getClass().getSimpleName()).isEqualTo("MethodHandleSpBroadcastByteRingBuffer");
        assertThat(multi.getClass().getSimpleName()).isEqualTo("MethodHandleMpBroadcastByteRingBuffer");
    }
}
