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


import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.function.Supplier;

import static org.lolaf.ringos.threading.InternalThreadLocalMap.VARIABLES_TO_REMOVE_INDEX;

/**
 * A {@link ThreadLocal}-like variable that a {@link FastThreadLocalThread} resolves by array index instead of a
 * hash-map probe, which makes a lookup around 50% faster than the JDK's.
 *
 * <p>Each instance claims an index at construction, and every thread's values live in one array addressed by those
 * indices. That is what to design for: the index space is global and never reclaimed, so a {@code FastThreadLocal}
 * belongs in a {@code static final} field, and creating them per request leaks index space and grows every
 * thread's array.
 *
 * <p>On a plain thread it still works — the values then hang off a regular {@link ThreadLocal} — but the array
 * lookup, and hence the point of the class, only applies on a {@link FastThreadLocalThread}.
 *
 * <p>Values set on a thread survive until {@link #remove()}, {@link #removeAll()}, or the thread finishing a body
 * that a {@link FastThreadLocalThread} wrapped. A thread that outlives the values it holds — a pooled one above
 * all — must be cleaned explicitly, or it pins whatever they reference.
 *
 * @param <V> type of the value held per thread
 */
public class FastThreadLocal<V> {

    private final int index;

    /**
     * Claims this variable's index, permanently. See the class javadoc: instances are meant to be few and
     * long-lived.
     *
     * @throws IllegalStateException if the index space is exhausted, which takes creating billions of instances
     */
    public FastThreadLocal() {
        index = InternalThreadLocalMap.nextVariableIndex();
    }

    /**
     * Removes every {@code FastThreadLocal} value held by the calling thread, calling {@link #onRemoval(Object)}
     * on each, then detaches the thread's map altogether.
     * <p>
     * Called automatically when a {@link FastThreadLocalThread} finishes its body; call it by hand at the end of
     * a task that ran on a thread the pool will reuse.
     */
    public static void removeAll() {
        InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.getIfSet();
        if (threadLocalMap == null) {
            return;
        }
        try {
            Object v = threadLocalMap.indexedVariable(VARIABLES_TO_REMOVE_INDEX);
            if (v != null && v != InternalThreadLocalMap.UNSET) {
                @SuppressWarnings("unchecked")
                Set<FastThreadLocal<?>> variablesToRemove = (Set<FastThreadLocal<?>>) v;
                FastThreadLocal<?>[] variablesToRemoveArray = variablesToRemove.toArray(new FastThreadLocal[0]);
                for (FastThreadLocal<?> tlv : variablesToRemoveArray) {
                    tlv.remove(threadLocalMap);
                }
            }
        } finally {
            InternalThreadLocalMap.remove();
        }
    }

    /**
     * @return how many {@code FastThreadLocal} variables currently hold a value for the calling thread, {@code 0}
     * if it has no map at all. For diagnostics — a count that only grows across a pooled thread's tasks points at
     * a missing {@link #removeAll()}
     */
    public static int size() {
        InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.getIfSet();
        return threadLocalMap != null ? threadLocalMap.size() : 0;
    }

    /**
     * Drops the fallback map the calling thread holds through the regular {@link ThreadLocal}, without running
     * {@link #onRemoval(Object)} on the values in it.
     * <p>
     * Only concerns threads that are not {@link FastThreadLocalThread}s, which carry their map in a field
     * instead. {@link #removeAll()} is the orderly way to release values; this is the blunt one, for shutting a
     * classloader or a container down.
     */
    public static void destroy() {
        InternalThreadLocalMap.destroy();
    }

    @SuppressWarnings("unchecked")
    private static void addToVariablesToRemove(InternalThreadLocalMap threadLocalMap, FastThreadLocal<?> variable) {
        Object v = threadLocalMap.indexedVariable(VARIABLES_TO_REMOVE_INDEX);
        Set<FastThreadLocal<?>> variablesToRemove;
        if (v == InternalThreadLocalMap.UNSET || v == null) {
            variablesToRemove = Collections.newSetFromMap(new IdentityHashMap<>());
            threadLocalMap.setIndexedVariable(VARIABLES_TO_REMOVE_INDEX, variablesToRemove);
        } else {
            variablesToRemove = (Set<FastThreadLocal<?>>) v;
        }
        variablesToRemove.add(variable);
    }

    private static void removeFromVariablesToRemove(InternalThreadLocalMap threadLocalMap, FastThreadLocal<?> variable) {
        Object v = threadLocalMap.indexedVariable(VARIABLES_TO_REMOVE_INDEX);
        if (v == InternalThreadLocalMap.UNSET || v == null) {
            return;
        }
        @SuppressWarnings("unchecked")
        Set<FastThreadLocal<?>> variablesToRemove = (Set<FastThreadLocal<?>>) v;
        variablesToRemove.remove(variable);
    }

    /**
     * Builds a variable whose per-thread value is produced on first {@link #get()}, the lambda form of
     * overriding {@link #initialValue()}.
     *
     * @param initialValue called once per thread that reads the variable without having set it
     * @param <V>          type of the value held per thread
     * @return the new variable, which claims an index like any other — see the class javadoc
     */
    public static <V> FastThreadLocal<V> withInitial(Supplier<V> initialValue) {
        return new FastThreadLocal<>() {
            @Override
            protected V initialValue() {
                return initialValue.get();
            }
        };
    }

    /**
     * Returns the current value for the current thread, initialising it through {@link #initialValue()} on first
     * read.
     *
     * @return the value held for the calling thread
     */
    @SuppressWarnings("unchecked")
    public final V get() {
        InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.get();
        Object v = threadLocalMap.indexedVariable(index);
        if (v != InternalThreadLocalMap.UNSET) {
            return (V) v;
        }

        return initialize(threadLocalMap);
    }

    /**
     * Returns the current value for the current thread if it exists, {@code null} otherwise. Unlike
     * {@link #get()}, it does not initialise the variable.
     *
     * @return the value held for the calling thread, or {@code null} if it has none
     */
    @SuppressWarnings("unchecked")
    public final V getIfExists() {
        InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.getIfSet();
        if (threadLocalMap != null) {
            Object v = threadLocalMap.indexedVariable(index);
            if (v != InternalThreadLocalMap.UNSET) {
                return (V) v;
            }
        }
        return null;
    }

    private V initialize(InternalThreadLocalMap threadLocalMap) {
        V initialValue = initialValue();
        threadLocalMap.setIndexedVariable(index, initialValue);
        addToVariablesToRemove(threadLocalMap, this);
        return initialValue;
    }

    /**
     * Set the value for the current thread.
     *
     * @param value the value to hold for the calling thread; {@code null} is stored as a value in its own right,
     *              which is not the same as {@link #remove() removing} the variable
     */
    public final void set(V value) {
        InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.get();
        if (threadLocalMap.setIndexedVariable(index, value)) {
            addToVariablesToRemove(threadLocalMap, this);
        }
    }

    /**
     * Returns {@code true} if and only if this thread-local variable is set.
     *
     * @return {@code true} if the calling thread holds a value, set or initialised
     */
    public final boolean isSet() {
        InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.getIfSet();
        return threadLocalMap != null && threadLocalMap.isIndexedVariableSet(index);
    }

    /**
     * Sets the value to uninitialized for the specified thread local map.
     * After this, any subsequent call to get() will trigger a new call to initialValue().
     */
    public final void remove() {
        remove(InternalThreadLocalMap.getIfSet());
    }

    @SuppressWarnings("unchecked")
    private void remove(InternalThreadLocalMap threadLocalMap) {
        if (threadLocalMap == null) {
            return;
        }

        Object v = threadLocalMap.removeIndexedVariable(index);
        if (v != InternalThreadLocalMap.UNSET) {
            removeFromVariablesToRemove(threadLocalMap, this);
            try {
                onRemoval((V) v);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }

    /**
     * Returns the initial value for this thread-local variable. Called once per thread, on the first {@link #get()}
     * that finds nothing; override it, or use {@link #withInitial(Supplier)}.
     *
     * @return the initial value, {@code null} unless overridden
     */
    protected V initialValue() {
        return null;
    }

    /**
     * Invoked when this thread local variable is removed by {@link #remove()}. Be aware that {@link #remove()}
     * is not guaranteed to be called when the `Thread` completes which means you can not depend on this for
     * cleanup of the resources in the case of `Thread` completion.
     *
     * @param value the value being removed
     */
    protected void onRemoval(@SuppressWarnings("UnusedParameters") V value) {
        // override me
    }
}
