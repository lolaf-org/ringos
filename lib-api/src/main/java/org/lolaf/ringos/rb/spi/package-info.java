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

/**
 * <b>Internal.</b> The skeleton the bundled {@link org.lolaf.ringos.rb.RingBuffer} implementations are
 * built on, and not part of the API this library supports.
 *
 * <p>Applications use {@link org.lolaf.ringos.rb.RingBufferFactory}; nothing here is meant to be called,
 * extended or referenced from outside ringos. The types are public only because the implementations live
 * in their own packages — {@code org.lolaf.ringos.rb.unsafe} and its siblings — and cross-package
 * inheritance needs it. That in turn is what stops the ring-buffer jars from contributing the same
 * package, which the module system forbids.
 *
 * <p>What is here is the claim protocol itself: the backing array and its padding, the head and tail
 * counters, the compare-and-swap and memory-ordering primitives each implementation supplies. Its shape
 * follows whatever the fastest correct implementation needs, so <b>it may change in any release,
 * including a patch release</b>. Depend on it and an upgrade will break you.
 */
package org.lolaf.ringos.rb.spi;
