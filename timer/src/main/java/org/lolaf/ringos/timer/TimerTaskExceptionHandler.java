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
 * Notified when a timer task, or a task's completion callback, throws.
 *
 * <p>This is the timer's backstop against a silent failure. The completion callback of
 * {@link WheelTimer#schedule(Runnable, java.util.function.BiConsumer, long, java.util.concurrent.TimeUnit)}
 * reports a {@code Runnable}'s outcome to whoever scheduled it, but the
 * {@link TimerTaskOneArg}/{@link TimerTaskTwoArg}/{@link TimerTaskThreeArg} overloads carry no callback — so
 * without this handler an exception from one of them would vanish entirely. It is also what reports an
 * {@link Error}, which the worker swallows rather than dying on, and an exception thrown by a completion
 * callback itself.
 *
 * <p>The default handler logs at error level. Replace it through
 * {@link WheelTimer#setTaskExceptionHandler(TimerTaskExceptionHandler)} to route failures into your own
 * metrics or alerting.
 *
 * <p>It runs on the tick/worker thread, so it must be cheap and non-blocking — the same rule that applies to
 * the tasks themselves. A handler that throws has its {@link Throwable} caught and discarded, because there is
 * nowhere left to report it to.
 */
@FunctionalInterface
public interface TimerTaskExceptionHandler {

    /**
     * @param task      the task that failed — a {@link Runnable}, {@link TimerTaskOneArg},
     *                  {@link TimerTaskTwoArg} or {@link TimerTaskThreeArg}, depending on which overload
     *                  scheduled it. It is the completion callback itself when that is what threw
     * @param exception what it threw
     */
    void onException(Object task, Throwable exception);
}
