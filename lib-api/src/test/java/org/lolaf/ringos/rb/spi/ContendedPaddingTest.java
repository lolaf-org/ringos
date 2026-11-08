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
package org.lolaf.ringos.rb.spi;

import org.junit.jupiter.api.Test;
import org.lolaf.ringos.unsafe.UnsafeOperations;
import org.mockito.Mockito;
import org.slf4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class ContendedPaddingTest {

    private static final int CACHE_LINE = 64;

    private final Logger log = Mockito.mock(Logger.class);

    private List<String> reported(long firstOffset, long secondOffset) {
        ContendedPadding.report(log, "head", "tail", firstOffset, secondOffset, CACHE_LINE);
        return Mockito.mockingDetails(log).getInvocations().stream()
                .map(invocation -> invocation.getMethod().getName())
                .collect(Collectors.toList());
    }

    @Test
    void fieldsACacheLineApartAreNotReported() {
        assertThat(reported(16, 16 + CACHE_LINE)).isEmpty();
    }

    @Test
    void fieldsOnOneCacheLineAreAnErrorWhicheverComesFirst() {
        assertThat(reported(24, 16)).containsExactly("error");
    }

    @Test
    void anUnknownLayoutIsOnlyInformation() {
        assertThat(reported(UnsafeOperations.UNKNOWN_FIELD_OFFSET, 16)).containsExactly("info");
    }
}
