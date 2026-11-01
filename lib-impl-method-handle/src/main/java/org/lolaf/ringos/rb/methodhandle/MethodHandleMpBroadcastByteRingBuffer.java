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
package org.lolaf.ringos.rb.methodhandle;

import org.lolaf.ringos.rb.ByteTranslator;

import java.nio.ByteBuffer;

/**
 * A multi-producer broadcast byte ring buffer: the single-producer protocol, with each write behind a spin lock.
 */
public class MethodHandleMpBroadcastByteRingBuffer extends MethodHandleSpBroadcastByteRingBuffer {

    public MethodHandleMpBroadcastByteRingBuffer(int capacity) {
        super(capacity);
    }

    @Override
    public <A> long write(int type, int length, ByteTranslator<A> translator, A arg) {
        lockWriters();
        try {
            return super.write(type, length, translator, arg);
        } finally {
            unlockWriters();
        }
    }

    @Override
    public long write(int type, ByteBuffer src, int srcIndex, int length) {
        lockWriters();
        try {
            return super.write(type, src, srcIndex, length);
        } finally {
            unlockWriters();
        }
    }
}
