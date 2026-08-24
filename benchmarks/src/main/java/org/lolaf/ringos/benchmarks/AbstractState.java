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
 * The queue under measurement, and the two operations the benchmark methods drive it with.
 */
abstract class AbstractState {

    static final int MAX_POLL_ITERATIONS = 32 * 1024;

    RingBufferInterface<Object> buffer;

    private final Object testValue = new Object();

    Object poll() {
        Object result;
        int pollIterations = 0;
        while ((result = buffer.poll()) == null) {
            Thread.onSpinWait();
            if (pollIterations++ > MAX_POLL_ITERATIONS) {
                int currentSize = buffer.getSize();
                if (currentSize == 0) {
                    return null;
                }
                Thread.yield();
                pollIterations = 0;
                System.out.println("Cannot poll data for rb " + buffer.getClass().getSimpleName() + " " + buffer.getSize());
            }
        }
        return result;
    }

    boolean offer() {
        return buffer.offer(testValue);
    }
}
