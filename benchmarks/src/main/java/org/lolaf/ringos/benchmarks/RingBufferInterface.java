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
package org.lolaf.ringos.benchmarks;

/**
 * The operations every benchmarked queue is driven through, so that the ring buffers and the
 * third-party queues they are compared against can be measured behind one call shape.
 *
 * @param <T> the element type
 */
public interface RingBufferInterface<T> {

    boolean offer(T item);

    T poll();

    int getSize();

    void clear();
}
