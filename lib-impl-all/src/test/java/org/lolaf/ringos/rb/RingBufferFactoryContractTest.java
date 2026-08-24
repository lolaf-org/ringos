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

import org.lolaf.ringos.rb.testkit.AbstractRingBufferContractTest;

/**
 * Runs the shared contract against whatever {@link RingBufferFactory} selects, rather than against a builder
 * named in the source. The module runs this twice — with and without
 * {@code --add-opens java.base/jdk.internal.misc=ALL-UNNAMED} — so both branches of the selection produce
 * buffers that actually work, which {@link RingBufferImplAllTest} on its own only asserts by class name.
 *
 * <p>Only the single-threaded contract runs here: the concurrency kit is already run against each
 * implementation in its own module, and running it again under both of this module's surefire executions would
 * cost four more passes to prove nothing new.
 */
class RingBufferFactoryContractTest extends AbstractRingBufferContractTest {

    @Override
    protected RingBufferBuilder builder() {
        return RingBufferFactory::build;
    }
}
