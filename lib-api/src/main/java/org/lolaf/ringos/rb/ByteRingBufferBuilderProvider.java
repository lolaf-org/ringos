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
 * Selects the {@link ByteRingBufferBuilder} for the current runtime, by the same rules as the object ring buffers'
 * providers, whose methods it takes: {@link #implementationClassName()} names a {@link ByteRingBufferBuilder}. A
 * separate service, so an implementation module may provide one kind of ring buffer without the other.
 */
public interface ByteRingBufferBuilderProvider extends RingBufferBuilderProvider {
}
