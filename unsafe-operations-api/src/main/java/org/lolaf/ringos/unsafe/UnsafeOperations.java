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
package org.lolaf.ringos.unsafe;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

/**
 * Narrow façade over the JDK-internal {@code Unsafe} operations ringos needs: direct buffer
 * deallocation, field/array layout offsets, and raw field access at those offsets.
 * <p>
 * <b>Getting one.</b> Never instantiate an implementation directly — the class that can be loaded
 * depends on the running JDK. Go through {@link UnsafeOperationsApi}, which picks the right one via
 * {@link UnsafeOperationsProvider} and reports cleanly when none applies:
 * <pre>{@code
 * int lineSize = UnsafeOperationsApi.ifAvailableDoReturn(
 *         UnsafeOperations::getL1CacheLineSize,
 *         UnsafeOperations.DEFAULT_L1_CACHE_LINE_SIZE);
 * }</pre>
 * Unsafe is not always usable — a required {@code --add-opens} may be missing, or the JDK may be
 * outside every supported range — so every caller needs a fallback path; see
 * {@link UnsafeOperationsApi#isAvailable()}.
 * <p>
 * <b>Safety.</b> These methods bypass every check the language gives you. An offset that did not
 * come from {@link #objectFieldOffset(Class, String) objectFieldOffset} or
 * {@link #arrayBaseOffset(Class) arrayBaseOffset}/{@link #arrayIndexScale(Class) arrayIndexScale} for the
 * very object being accessed corrupts memory or crashes the JVM; there is no exception to catch.
 * Offsets are also specific to one JVM run and one class — cache them in a {@code static final}
 * next to the class they describe, never persist or share them.
 * <p>
 * <b>Memory ordering.</b> Only {@link #getAndSetInt(Object, long, int) getAndSetInt} and
 * {@link #getAndSetReference(Object, long, Object) getAndSetReference} are atomic and
 * ordered (volatile semantics). The {@code get*}/{@code put*} pairs are <em>plain</em> accesses:
 * they carry no happens-before edge, so a value written by one thread may never become visible to
 * another. Use them for single-threaded work, or where the ordering is established by some other
 * fence.
 * <p>
 * Implementations are stateless and safe to share across threads.
 *
 * @see UnsafeOperationsApi
 * @see UnsafeOperationsProvider
 */
public interface UnsafeOperations {

    /**
     * L1 cache line size, in bytes, to assume when the runtime cannot report the real one — the
     * width every mainstream x86-64 and AArch64 core uses. Pair it with {@link #getL1CacheLineSize()}
     * as the fallback for when Unsafe is unavailable altogether.
     */
    int DEFAULT_L1_CACHE_LINE_SIZE = 64;

    /**
     * Returned by {@link #objectFieldOffset(Class, String)} when the class declares no such field.
     * Negative on purpose, so that using it unchecked as an offset fails loudly rather than reading
     * a neighbouring field.
     */
    long UNKNOWN_FIELD_OFFSET = -1;

    /**
     * Releases {@code byteBuffer}'s off-heap memory immediately if it is direct, and does nothing if
     * it is a heap buffer. The forgiving variant of {@link #invokeCleaner(ByteBuffer)}, and the one
     * to reach for when the buffer's kind is not statically known.
     * <p>
     * The memory is gone when this returns: any later read or write through {@code byteBuffer}, or
     * through a slice or duplicate of it, touches freed memory and can crash the JVM. Only call this
     * on a buffer you own outright and will not hand out again.
     *
     * @param byteBuffer the buffer to free; must not be a slice or duplicate of a buffer still in use
     */
    void invokeCleanerIfNeeded(ByteBuffer byteBuffer);

    /**
     * Releases {@code byteBuffer}'s off-heap memory immediately. Same contract and the same
     * use-after-free hazard as {@link #invokeCleanerIfNeeded(ByteBuffer)}, except that the buffer
     * must be direct: passing a heap buffer throws rather than being ignored.
     *
     * @param byteBuffer the direct buffer to free
     * @throws RuntimeException if {@code byteBuffer} is not direct, or its cleaner cannot be reached
     */
    void invokeCleaner(ByteBuffer byteBuffer);

    /**
     * Locates a field in {@code clazz}'s instance layout by name. The lookup covers declared fields
     * only — an inherited field is not found — and the offset is valid solely for instances of
     * {@code clazz} in this JVM run.
     *
     * @param clazz the class declaring the field
     * @param field the field's name
     * @return the offset to pass to the {@code get*}/{@code put*} methods, or
     * {@link #UNKNOWN_FIELD_OFFSET} if {@code clazz} declares no field by that name. Callers must
     * test for that sentinel and fall back, since the JDK is free to hide or rename its internals
     * between releases
     */
    long objectFieldOffset(Class<?> clazz, String field);

    /**
     * Locates an already-resolved field in its declaring class's instance layout. Unlike
     * {@link #objectFieldOffset(Class, String)} there is no sentinel: a field this JVM refuses to
     * expose raises instead.
     *
     * @param field the instance field to locate; static fields are not supported
     * @return the offset to pass to the {@code get*}/{@code put*} methods
     * @throws RuntimeException if the field's offset cannot be taken
     */
    long objectFieldOffset(Field field);

    /**
     * @param arrayClass the array class, e.g. {@code long[].class}
     * @return the offset of element 0 within an array of that class. Combined with
     * {@link #arrayIndexScale(Class)}, element {@code i} sits at
     * {@code arrayBaseOffset + (long) i * arrayIndexScale}
     */
    long arrayBaseOffset(Class<?> arrayClass);

    /**
     * @param arrayClass the array class, e.g. {@code long[].class}
     * @return the number of bytes between consecutive elements of that array class — 4 for
     * {@code int[]}, and, for reference arrays, 4 or 8 depending on whether compressed oops are on
     * @see #arrayBaseOffset(Class)
     */
    int arrayIndexScale(Class<?> arrayClass);

    /**
     * Atomically stores {@code value} at {@code offset} and returns what was there before, with
     * volatile ordering on both halves.
     *
     * @param obj    the object (or array) holding the slot
     * @param offset the slot's offset, from {@link #objectFieldOffset(Class, String) objectFieldOffset} or the array formula
     * @param value  the reference to store
     * @param <T>    the reference type held in the slot
     * @return the reference the slot held before this call
     */
    <T> T getAndSetReference(Object obj, long offset, T value);

    /**
     * Atomically stores {@code value} at {@code offset} and returns what was there before, with
     * volatile ordering on both halves.
     *
     * @param obj    the object (or array) holding the slot
     * @param offset the slot's offset, from {@link #objectFieldOffset(Class, String) objectFieldOffset} or the array formula
     * @param value  the {@code int} to store
     * @return the {@code int} the slot held before this call
     */
    int getAndSetInt(Object obj, long offset, int value);

    /**
     * Plain read of a reference slot — no ordering, no visibility guarantee against other threads.
     *
     * @param obj    the object (or array) holding the slot
     * @param offset the slot's offset, from {@link #objectFieldOffset(Class, String) objectFieldOffset} or the array formula
     * @param <T>    the reference type held in the slot; unchecked, so a wrong {@code T} surfaces as
     *               a {@link ClassCastException} at the assignment rather than here
     * @return the reference currently in the slot
     */
    <T> T getReference(Object obj, long offset);

    /**
     * Plain write of a reference slot — no ordering, no visibility guarantee against other threads.
     *
     * @param object the object (or array) holding the slot
     * @param offset the slot's offset, from {@link #objectFieldOffset(Class, String) objectFieldOffset} or the array formula
     * @param value  the reference to store; must be assignable to the slot's declared type, which is
     *               not checked
     */
    void putReference(Object object, long offset, Object value);

    /**
     * Plain write of an {@code int} slot — no ordering, no visibility guarantee against other threads.
     *
     * @param obj    the object (or array) holding the slot
     * @param offset the slot's offset, from {@link #objectFieldOffset(Class, String) objectFieldOffset} or the array formula
     * @param value  the value to store
     */
    void putInt(Object obj, long offset, int value);

    /**
     * Plain write of a {@code long} slot — no ordering, no visibility guarantee against other
     * threads, and on a 32-bit VM not even atomic.
     *
     * @param obj    the object (or array) holding the slot
     * @param offset the slot's offset, from {@link #objectFieldOffset(Class, String) objectFieldOffset} or the array formula
     * @param value  the value to store
     */
    void putLong(Object obj, long offset, long value);

    /**
     * The L1 cache line width to align to, or pad by, so that two hot fields do not share a line.
     * What the runtime can tell us varies: on Java 11-14 there is no way to ask, so
     * {@link #DEFAULT_L1_CACHE_LINE_SIZE} is returned; from Java 15 the CPU's real flush size is
     * reported, which is {@code 0} on a CPU that cannot report one.
     *
     * @return the cache line size in bytes, or {@code 0} if the CPU does not report one — treat a
     * non-positive result as {@link #DEFAULT_L1_CACHE_LINE_SIZE}
     */
    int getL1CacheLineSize();
}
