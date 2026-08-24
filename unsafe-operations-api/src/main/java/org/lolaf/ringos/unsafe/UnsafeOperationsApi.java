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

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Entry point to the {@link UnsafeOperations} implementation for the running JDK. This is the only
 * supported way to obtain one; the implementations themselves must never be named directly, because
 * which of them can even be <em>loaded</em> depends on the JDK in use — see
 * {@link UnsafeOperationsProvider} for why.
 * <p>
 * Selection happens once, in this class's static initializer: every {@link UnsafeOperationsProvider}
 * on the classpath is asked whether it targets the running JDK, the winner's required
 * {@code --add-opens} are verified, and only then is its implementation loaded reflectively.
 * <p>
 * <b>Failure is not exceptional.</b> Unsafe is genuinely unavailable on some runtimes — a missing
 * {@code --add-opens}, an unsupported JDK, no provider on the classpath — so the reason is captured
 * at class-init instead of thrown. Merely touching this class therefore never fails, and callers
 * choose how to react:
 * <ul>
 *   <li>{@link #ifAvailableDo(Consumer)} / {@link #ifAvailableDoReturn(Function, Object)} — do the
 *       Unsafe-backed thing when possible, silently skip it or fall back when not. This is what hot
 *       paths with a plain-Java alternative should use.</li>
 *   <li>{@link #isAvailable()} — branch on it explicitly.</li>
 *   <li>{@link #get()} — take the implementation or blow up. Only for code that has no fallback.</li>
 * </ul>
 * To run with Unsafe enabled, start the JVM with the {@code --add-opens} each implementation
 * declares through {@link UnsafeOperationsProvider#requiredOpenPackages()}; the message on
 * {@link MissingAddOpensException} spells out the exact flags.
 *
 * @see UnsafeOperations
 * @see UnsafeOperationsProvider
 */
@Slf4j
@UtilityClass
public class UnsafeOperationsApi {

    private static final UnsafeOperations UNSAFE_OPERATIONS;
    // Reason UNSAFE_OPERATIONS could not be built (missing --add-opens, unsupported JDK, no provider). Non-null
    // exactly when UNSAFE_OPERATIONS is null. Captured rather than thrown so that merely touching this class —
    // e.g. to call isAvailable() — never fails; get() re-throws it on demand.
    private static final RuntimeException UNAVAILABLE_CAUSE;

    static {
        UnsafeOperations unsafeOperations = null;
        RuntimeException unavailableCause = null;
        try {
            unsafeOperations = load();
        } catch (RuntimeException ex) {
            unavailableCause = ex;
            log.warn("UnsafeOperations is not available on this runtime: {}", ex.getMessage());
        }
        UNSAFE_OPERATIONS = unsafeOperations;
        UNAVAILABLE_CAUSE = unavailableCause;
    }

    private static UnsafeOperations load() {
        // Providers are lightweight and compiled to the lowest supported class-file version, so every one of
        // them loads on every supported JDK. We ask each whether it targets the running JDK and only then let
        // the winning provider reflectively load its (potentially newer-bytecode) UnsafeOperations class — so a
        // JDK-25 implementation never needs to be loaded on, say, a JDK 21 runtime. See UnsafeOperationsProvider.
        List<String> providers = new ArrayList<>();
        UnsafeOperationsProvider chosen = null;
        for (UnsafeOperationsProvider provider : ServiceLoader.load(UnsafeOperationsProvider.class)) {
            providers.add(provider.getClass().getName());
            if (chosen == null && provider.isForCurrentJDK()) {
                chosen = provider;
            }
        }
        if (providers.isEmpty()) {
            throw new IllegalStateException("Unable to find any UnsafeOperationsProvider SPI in classpath");
        }
        if (chosen == null) {
            throw new WrongJDKException(Runtime.version().feature());
        }
        // The JDK matches; now verify the implementation's required --add-opens are present, so a missing flag
        // surfaces as a clear MissingAddOpensException instead of a deep reflective init failure.
        checkRequiredOpens(chosen);
        UnsafeOperations unsafeOperations = instantiate(chosen.implementationClassName());
        log.info("Loaded UnsafeOperations SPI {}", unsafeOperations.getClass().getName());
        return unsafeOperations;
    }

    private static void checkRequiredOpens(UnsafeOperationsProvider provider) {
        Module javaBase = Object.class.getModule();
        Module self = provider.getClass().getModule();
        List<String> missing = new ArrayList<>();
        for (String pkg : provider.requiredOpenPackages()) {
            if (!javaBase.isOpen(pkg, self)) {
                missing.add("java.base/" + pkg + "=ALL-UNNAMED");
            }
        }
        if (!missing.isEmpty()) {
            throw new UnsafeOperationsApi.MissingAddOpensException(missing);
        }
    }

    // Reflectively builds the version-specific implementation the chosen provider named. The class is
    // referenced only by string (never linked by a provider), so a newer-bytecode implementation is loaded
    // here — and only after its provider confirmed the running JDK can load it.
    private static UnsafeOperations instantiate(String className) {
        try {
            return (UnsafeOperations) Class.forName(className).getDeclaredConstructor().newInstance();
        } catch (ExceptionInInitializerError | InvocationTargetException err) {
            // The implementation acquires Unsafe in its static initializer; a missing --add-opens surfaces
            // here wrapped in ExceptionInInitializerError. Unwrap so callers see the original
            // MissingAddOpensException rather than an opaque initializer error.
            throw asRuntimeException(err.getCause(), className);
        } catch (ReflectiveOperationException err) {
            throw new IllegalStateException("Unable to instantiate UnsafeOperations implementation " + className, err);
        }
    }

    private static RuntimeException asRuntimeException(Throwable cause, String className) {
        if (cause instanceof RuntimeException) {
            return (RuntimeException) cause;
        }
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        return new IllegalStateException("Unable to instantiate UnsafeOperations implementation " + className, cause);
    }

    /**
     * @return the implementation for this runtime, never {@code null}
     * @throws RuntimeException wrapping the reason Unsafe could not be set up — a
     *                          {@link MissingAddOpensException}, a {@link WrongJDKException}, or the
     *                          failure that stopped the implementation from loading. Prefer
     *                          {@link #ifAvailableDo(Consumer)} or {@link #isAvailable()} wherever a
     *                          fallback exists
     */
    public static UnsafeOperations get() {
        if (UNSAFE_OPERATIONS == null) {
            throw new RuntimeException(UNAVAILABLE_CAUSE);
        }
        return UNSAFE_OPERATIONS;
    }

    /**
     * @return {@code true} if {@link #get()} would return an implementation, {@code false} if it would throw
     * because Unsafe cannot be used on this runtime (e.g. a required {@code --add-opens} is missing). Callers
     * that have a non-Unsafe fallback can branch on this instead of catching {@link #get()}'s exception.
     */
    public static boolean isAvailable() {
        try {
            return get() != null;
        } catch (RuntimeException ex) {
            return false;
        }
    }

    /**
     * Hands the implementation to {@code runnable}, or does nothing at all if Unsafe is unavailable
     * on this runtime. The no-op case is silent: use it only where skipping the work is genuinely
     * harmless, such as an optimisation or a diagnostic.
     *
     * @param runnable the work to run against the implementation
     */
    public static void ifAvailableDo(Consumer<UnsafeOperations> runnable) {
        if (UNSAFE_OPERATIONS != null) {
            runnable.accept(UNSAFE_OPERATIONS);
        }
    }

    /**
     * Variant of {@link #ifAvailableDo(Consumer)} that passes an extra argument through, so the
     * callback can be a constant rather than a lambda capturing {@code param}.
     *
     * @param runnable the work to run against the implementation
     * @param param    the argument handed to {@code runnable} alongside the implementation
     * @param <T>      the argument type
     */
    public static <T> void ifAvailableDo(BiConsumer<UnsafeOperations, T> runnable, T param) {
        if (UNSAFE_OPERATIONS != null) {
            runnable.accept(UNSAFE_OPERATIONS, param);
        }
    }

    /**
     * Computes a value from the implementation, falling back to {@code defaultValue} when Unsafe is
     * unavailable. The usual shape for reading a machine property that has a sane default:
     * <pre>{@code
     * UnsafeOperationsApi.ifAvailableDoReturn(
     *         UnsafeOperations::getL1CacheLineSize,
     *         UnsafeOperations.DEFAULT_L1_CACHE_LINE_SIZE);
     * }</pre>
     *
     * @param producer     computes the value from the implementation
     * @param defaultValue returned instead when there is no implementation
     * @param <T>          the value type
     * @return {@code producer}'s result, or {@code defaultValue}
     */
    public static <T> T ifAvailableDoReturn(Function<UnsafeOperations, T> producer, T defaultValue) {
        if (UNSAFE_OPERATIONS != null) {
            return producer.apply(UNSAFE_OPERATIONS);
        }
        return defaultValue;
    }

    /**
     * Raised when the JDK for this runtime is supported but the JVM was not started with the
     * {@code --add-opens} its implementation needs. The message lists the missing flags verbatim, so
     * it can be pasted onto a command line.
     */
    class MissingAddOpensException extends RuntimeException {
        /**
         * @param addOpens the missing opens, each already formatted as
         *                 {@code java.base/<pkg>=ALL-UNNAMED}
         */
        public MissingAddOpensException(List<String> addOpens) {
            super(generateErrorMessage(addOpens));
        }

        private static String generateErrorMessage(List<String> addOpens) {
            StringBuilder msg = new StringBuilder("Missing JVM parameter ");
            addOpens.forEach(ao -> msg.append("--add-opens ").append(ao).append(" "));
            return msg.append("to unlock UnsafeOperations API").toString();
        }
    }

    /**
     * Raised when no {@link UnsafeOperationsProvider} on the classpath claims the running JDK —
     * either the JDK is newer than any implementation supports, or only some of the
     * {@code unsafe-operations-impl-*} modules are on the classpath. Depending on
     * {@code org.lolaf.ringos:unsafe-operations-impl-all} covers every supported range.
     */
    public class WrongJDKException extends RuntimeException {
        /**
         * @param jdkVersion the feature version of the running JDK, as reported by
         *                   {@link Runtime.Version#feature()}
         */
        public WrongJDKException(int jdkVersion) {
            super(generateErrorMessage(jdkVersion));
        }

        private static String generateErrorMessage(int jdkVersion) {
            return new StringBuilder("No UnsafeOperations implementation supports the running Java runtime ")
                    .append(jdkVersion).append(".\n")
                    .append("Supported ranges (bundled in org.lolaf.ringos:unsafe-operations-impl-all):\n")
                    .append("\tJava 11 to 14\n")
                    .append("\tJava 15 to 24\n")
                    .append("\tJava 25+\n").toString();
        }
    }
}