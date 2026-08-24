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
package org.lolaf.ringos.timer;

/**
 * A timer task that accepts three arguments, analogous to
 * {@link org.lolaf.ringos.rb.RingBuffer.EventTranslatorThreeArg} for ring buffer offers.
 * Storing the translator as a static field and passing the arguments separately avoids
 * allocating a capturing lambda on every {@link WheelTimer#schedule} call.
 *
 * @param <A> the first argument type
 * @param <B> the second argument type
 * @param <C> the third argument type
 */
@FunctionalInterface
public interface TimerTaskThreeArg<A, B, C> {

    /**
     * Runs the task.
     *
     * @param arg1 the first argument supplied at schedule time
     * @param arg2 the second argument supplied at schedule time
     * @param arg3 the third argument supplied at schedule time
     */
    void execute(A arg1, B arg2, C arg3);
}
