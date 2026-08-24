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

/**
 * Lightweight SPI that selects the {@link RingBufferBuilder} implementation to use on the current runtime.
 *
 * <p>Unlike a plain {@link RingBufferBuilder}, a provider can be asked <em>whether</em> it applies before its
 * builder (and the ring-buffer classes it constructs) is loaded. This lets a single dependency
 * ({@code ringos-lib-impl-all}) bundle every implementation and pick one at runtime.
 *
 * <p><b>Eligibility is driven by two facts about the runtime:</b>
 * <ul>
 *   <li>whether {@code java.base/jdk.internal.misc} is opened to this code (i.e. the JVM was started with
 *       {@code --add-opens java.base/jdk.internal.misc=ALL-UNNAMED}) — see {@link #isJdkInternalMiscOpen()};
 *       the {@code Unsafe}-based builders require it, the {@code MethodHandle}-based one does not;</li>
 *   <li>the running Java feature version, which distinguishes the two {@code Unsafe} builders.</li>
 * </ul>
 *
 * <p><b>More than one provider may be eligible</b>, and {@link #priority()} decides between them: the
 * {@code MethodHandle} builder runs on every supported runtime, so it claims all of them and sits at
 * {@link #LAST_RESORT}, leaving an {@code Unsafe} builder to win wherever one applies. Stating eligibility
 * positively this way — rather than as "whatever the others cannot handle" — is what keeps a new provider from
 * having to encode the preconditions of every existing one.
 */
public interface RingBufferBuilderProvider {

    /**
     * The priority of a provider that has no reason to yield to another — the default.
     *
     * @see #priority()
     */
    int PREFERRED = 0;

    /**
     * The priority of a provider that must be selected only when nothing else is eligible.
     *
     * @see #priority()
     */
    int LAST_RESORT = Integer.MAX_VALUE;

    /**
     * @return {@code true} if the {@link RingBufferBuilder} this provider names <em>can</em> be used on the
     * current runtime. Implementations decide from {@link #isJdkInternalMiscOpen()} and
     * {@link Runtime#version()} only, and answer for themselves alone: whether a better builder also applies
     * is {@link #priority()}'s business, not this method's.
     */
    boolean isForCurrentRuntime();

    /**
     * Ranks this provider against the others that also claim the current runtime. <b>Lower wins</b>, so
     * {@link #PREFERRED} beats {@link #LAST_RESORT}; providers sharing a priority are settled by
     * {@link java.util.ServiceLoader} iteration order, which is not something to rely on.
     *
     * @return this provider's rank, {@link #PREFERRED} by default
     */
    default int priority() {
        return PREFERRED;
    }

    /**
     * @return the fully-qualified name of the {@link RingBufferBuilder} to instantiate. Returned as a string
     * (not a {@link Class}) so this provider never links the builder; {@link RingBufferFactory} loads it
     * reflectively, and only when {@link #isForCurrentRuntime()} is {@code true}.
     */
    String implementationClassName();

    /**
     * @return {@code true} if {@code java.base/jdk.internal.misc} is open to this provider's module — the
     * signal that the JVM was launched with {@code --add-opens java.base/jdk.internal.misc=ALL-UNNAMED}, which
     * the {@code Unsafe}-based builders need. Exception-free: it inspects the module graph rather than trying
     * (and failing) to access {@code Unsafe}.
     */
    default boolean isJdkInternalMiscOpen() {
        return Object.class.getModule().isOpen("jdk.internal.misc", getClass().getModule());
    }
}
