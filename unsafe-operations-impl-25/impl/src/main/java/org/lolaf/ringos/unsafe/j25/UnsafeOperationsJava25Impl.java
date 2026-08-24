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
package org.lolaf.ringos.unsafe.j25;

import org.lolaf.ringos.unsafe.UnsafeOperations;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

public class UnsafeOperationsJava25Impl implements UnsafeOperations {

    private static final jdk.internal.misc.Unsafe UNSAFE;

    static {
        try {
            UNSAFE = jdk.internal.misc.Unsafe.getUnsafe();
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public void invokeCleanerIfNeeded(ByteBuffer byteBuffer) {
        if (byteBuffer.isDirect()) {
            UNSAFE.invokeCleaner(byteBuffer);
        }
    }

    @Override
    public void invokeCleaner(ByteBuffer byteBuffer) {
        UNSAFE.invokeCleaner(byteBuffer);
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
        return (T) UNSAFE.getAndSetReference(obj, offset, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getReference(Object obj, long offset) {
        return (T) UNSAFE.getReference(obj, offset);
    }

    @Override
    public void putReference(Object object, long offset, Object value) {
        UNSAFE.putReference(object, offset, value);
    }

    @Override
    public int getL1CacheLineSize() {
        return UNSAFE.dataCacheLineFlushSize();
    }
}