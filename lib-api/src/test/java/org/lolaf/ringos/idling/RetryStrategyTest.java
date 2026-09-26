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
package org.lolaf.ringos.idling;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class RetryStrategyTest {

    @Test
    void idlingWhileDelegatesResetToTheIdleStrategy() {
        IdleStrategy idleStrategy = mock(IdleStrategy.class);

        RetryStrategy.idlingWhile(idleStrategy, () -> true).reset();

        verify(idleStrategy).reset();
        verifyNoMoreInteractions(idleStrategy);
    }

    @Test
    void idlingWhileIdlesBeforeEachRetryUntilToldToStop() {
        IdleStrategy idleStrategy = mock(IdleStrategy.class);
        AtomicBoolean keepTrying = new AtomicBoolean(true);
        RetryStrategy retryStrategy = RetryStrategy.idlingWhile(idleStrategy, keepTrying::get);

        assertThat(retryStrategy.awaitRetry()).isTrue();
        keepTrying.set(false);
        assertThat(retryStrategy.awaitRetry()).isFalse();

        verify(idleStrategy, times(2)).idle();
    }
}
