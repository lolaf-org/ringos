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

/**
 * Who may write to a buffer: the producer half of {@link RingBufferFactory.AccessType}, and the whole choice for a
 * {@link BroadcastByteRingBuffer}, whose readers never compete. <b>A contract, not a hint</b>: two threads writing a
 * {@link #SINGLE} buffer corrupt it silently.
 */
public enum ProducerType {
    /**
     * One thread writes, so nothing coordinates the writes. The fastest.
     */
    SINGLE,
    /**
     * Any number of threads write.
     */
    MULTI
}
