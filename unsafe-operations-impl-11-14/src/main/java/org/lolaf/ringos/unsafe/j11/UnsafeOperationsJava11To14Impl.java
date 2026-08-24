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
package org.lolaf.ringos.unsafe.j11;

import org.lolaf.ringos.unsafe.UnsafeOperations;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

public class UnsafeOperationsJava11To14Impl implements UnsafeOperations {

    private static final jdk.internal.misc.Unsafe UNSAFE;
    private static final String JDK9_CLEANER_CLASS_NAME = "jdk.internal.ref.Cleaner";
    private static final MethodHandle CLEANER_METHOD;
    private static final MethodHandle CLEAN_METHOD;

    static {
        UNSAFE = jdk.internal.misc.Unsafe.getUnsafe();
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        try {
            Class<?> cleanerClass = Class.forName(JDK9_CLEANER_CLASS_NAME);
            CLEANER_METHOD = lookup.findVirtual(Class.forName("sun.nio.ch.DirectBuffer"), "cleaner", MethodType.methodType(cleanerClass));
            CLEAN_METHOD = lookup.findVirtual(cleanerClass, "clean", MethodType.methodType(Void.TYPE));
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public void invokeCleanerIfNeeded(ByteBuffer byteBuffer) {
        if (byteBuffer.isDirect()) {
            invokeCleaner(byteBuffer);
        }
    }

    @Override
    public void invokeCleaner(ByteBuffer byteBuffer) {
        try {
            Object cleaner = CLEANER_METHOD.invoke(byteBuffer);
            CLEAN_METHOD.invoke(cleaner);
        } catch (Throwable t) {
            throw new IllegalStateException(t);
        }
    }

    @Override
    public long objectFieldOffset(Class<?> clazz, String fieldName) {
        try {
            return UNSAFE.objectFieldOffset(clazz.getDeclaredField(fieldName));
        } catch (NoSuchFieldException e) {
            return UnsafeOperations.UNKNOWN_FIELD_OFFSET;
        }
    }

    @Override
    public long objectFieldOffset(Field field) {
        return UNSAFE.objectFieldOffset(field);
    }

    @Override
    public long arrayBaseOffset(Class<?> arrayClass) {
        return UNSAFE.arrayBaseOffset(arrayClass);
    }

    @Override
    public int arrayIndexScale(Class<?> arrayClass) {
        return UNSAFE.arrayIndexScale(arrayClass);
    }

    @Override
    public int getAndSetInt(Object obj, long offset, int value) {
        return UNSAFE.getAndSetInt(obj, offset, value);
    }

    @Override
    public void putInt(Object obj, long offset, int value) {
        UNSAFE.putInt(obj, offset, value);
    }

    @Override
    public void putLong(Object obj, long offset, long value) {
        UNSAFE.putLong(obj, offset, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getAndSetReference(Object obj, long offset, T value) {
        return (T) UNSAFE.getAndSetObject(obj, offset, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getReference(Object obj, long offset) {
        return (T) UNSAFE.getObject(obj, offset);
    }

    @Override
    public void putReference(Object object, long offset, Object value) {
        UNSAFE.putObject(object, offset, value);
    }

    @Override
    public int getL1CacheLineSize() {
        // method does not exists in java 11, but most CPU have 64Kb of l1 cache line size
        return DEFAULT_L1_CACHE_LINE_SIZE;
    }
}