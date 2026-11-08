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

import lombok.experimental.UtilityClass;
import org.lolaf.ringos.unsafe.UnsafeOperations;
import org.lolaf.ringos.unsafe.UnsafeOperationsApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks that two {@code @Contended} fields really landed on separate cache lines. It measures the layout rather than
 * reading the JVM flags, which can be set in ways that never show on the command line.
 */
@UtilityClass
class ContendedPadding {

    static void check(Class<?> owner, String first, String second) {
        Logger log = LoggerFactory.getLogger(owner);
        if (!UnsafeOperationsApi.isAvailable()) {
            log.info("Unable to read this JVM's field layout, so @Contended padding could not be verified; set"
                    + " -XX:-RestrictContended and -XX:ContendedPaddingWidth=<your L1 cache line size> for"
                    + " optimal performance");
            return;
        }
        UnsafeOperationsApi.ifAvailableDo(uo -> report(log, first, second, uo.objectFieldOffset(owner, first),
                uo.objectFieldOffset(owner, second), uo.getL1CacheLineSize()));
    }

    static void report(Logger log, String first, String second, long firstOffset, long secondOffset, int cacheLineSize) {
        if (firstOffset == UnsafeOperations.UNKNOWN_FIELD_OFFSET || secondOffset == UnsafeOperations.UNKNOWN_FIELD_OFFSET) {
            log.info("Unable to read the ring buffer's field layout, so @Contended padding could not be"
                    + " verified; set -XX:-RestrictContended and -XX:ContendedPaddingWidth={} for"
                    + " optimal performance", cacheLineSize);
            return;
        }
        // the JVM lays fields out as it pleases, so neither offset is necessarily the lower of the two
        long separation = Math.abs(secondOffset - firstOffset);
        if (separation < cacheLineSize) {
            // over-padding is not reported: a width wider than the line wastes memory but costs no contention
            log.error("@Contended padding is not in effect: the ring buffer's {} and {} are {} bytes apart and so share"
                    + " a {}-byte cache line. Set -XX:-RestrictContended -XX:ContendedPaddingWidth={} for optimal"
                    + " performance", first, second, separation, cacheLineSize, cacheLineSize);
        }
    }
}
