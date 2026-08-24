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
package org.lolaf.ringos.threading;

import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

class FastThreadLocalTest {

    @Test
    void testIsSet() {
        FastThreadLocal<String> ftl = new FastThreadLocal<>();

        Assertions.assertThat(ftl.isSet()).isFalse();

        ftl.set("test");
        Assertions.assertThat(ftl.isSet()).isTrue();

        ftl.set(null);
        Assertions.assertThat(ftl.isSet()).isTrue();

        ftl.remove();
        Assertions.assertThat(ftl.isSet()).isFalse();
    }

    @Test
    void testMultipleThreadLocalWorks() {
        FastThreadLocal<String> ftl1 = FastThreadLocal.withInitial(() -> "test1");

        Assertions.assertThat(ftl1.isSet()).isFalse();
        Assertions.assertThat(ftl1.get()).isEqualTo("test1");
        Assertions.assertThat(ftl1.isSet()).isTrue();

        FastThreadLocal<String> ftl2 = FastThreadLocal.withInitial(() -> "test2");

        Assertions.assertThat(ftl2.isSet()).isFalse();
        Assertions.assertThat(ftl2.get()).isEqualTo("test2");
        Assertions.assertThat(ftl2.isSet()).isTrue();
    }

    @Test
    void testMultipleThreadLocalWorksFromFastThread() {

        Set<String> removed = Collections.synchronizedSet(new HashSet<>());

        new FastThreadLocalThread(() -> {
            FastThreadLocal<String> ftl1 = getStringFastThreadLocal("test1-", removed);
            FastThreadLocal<String> ftl2 = getStringFastThreadLocal("test2-", removed);
            Assertions.assertThat(ftl1.isSet()).isFalse();
            Assertions.assertThat(ftl1.get()).isEqualTo("test1-thread1");
            Assertions.assertThat(ftl1.isSet()).isTrue();
            Assertions.assertThat(ftl1.get()).isEqualTo("test1-thread1");
            Assertions.assertThat(ftl2.isSet()).isFalse();
            Assertions.assertThat(ftl2.get()).isEqualTo("test2-thread1");
            Assertions.assertThat(ftl2.isSet()).isTrue();
            Assertions.assertThat(ftl2.get()).isEqualTo("test2-thread1");
        }, "thread1").start();

        new FastThreadLocalThread(() -> {
            FastThreadLocal<String> ftl1 = getStringFastThreadLocal("test1-", removed);
            FastThreadLocal<String> ftl2 = getStringFastThreadLocal("test2-", removed);
            Assertions.assertThat(ftl1.isSet()).isFalse();
            Assertions.assertThat(ftl1.get()).isEqualTo("test1-thread2");
            Assertions.assertThat(ftl1.isSet()).isTrue();
            Assertions.assertThat(ftl1.get()).isEqualTo("test1-thread2");
            Assertions.assertThat(ftl2.isSet()).isFalse();
            Assertions.assertThat(ftl2.get()).isEqualTo("test2-thread2");
            Assertions.assertThat(ftl2.isSet()).isTrue();
            Assertions.assertThat(ftl2.get()).isEqualTo("test2-thread2");
        }, "thread2").start();

        Awaitility.await().untilAsserted(() -> Assertions.assertThat(removed).hasSize(4));

        Assertions.assertThat(removed)
                .contains("test1-thread1")
                .contains("test2-thread1")
                .contains("test1-thread2")
                .contains("test1-thread2");

    }

    private FastThreadLocal<String> getStringFastThreadLocal(String x, Set<String> removed) {
        return new FastThreadLocal<>() {
            @Override
            protected String initialValue() {
                return x + Thread.currentThread().getName();
            }

            @Override
            protected void onRemoval(String value) {
                removed.add(value);
            }
        };
    }

}
