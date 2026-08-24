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

import org.junit.jupiter.api.Test;

import java.lang.module.Configuration;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReference;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Every ringos jar must be usable on the module path, not only the classpath.
 *
 * <p>The rule this guards is that a package belongs to exactly one module. Ringos broke it for a long
 * time without anyone noticing: {@code org.lolaf.ringos.rb} was carried by four jars and
 * {@code org.lolaf.ringos} by six, because the implementations extend package-private base classes and
 * so had to share their package name. Nothing caught it, because every test ran on the classpath, where
 * the rule does not exist. An application with its own {@code module-info}, or anyone running
 * {@code jlink}, could not use the library at all:
 *
 * <pre>
 * java.lang.module.ResolutionException: Module ringos.lib.unsafe.impl contains package
 * org.lolaf.ringos.rb, module ringos.lib.api exports package org.lolaf.ringos.rb to
 * ringos.lib.unsafe.impl
 * </pre>
 *
 * <p>This resolves the jars in-process rather than forking a JVM, which is the same check the launcher
 * makes: {@link Configuration#resolve} rejects a configuration in which two modules contain the same
 * package, and raises exactly that exception.
 *
 * <p>It reads {@code target/module-path}, filled by {@code maven-dependency-plugin} at
 * {@code process-test-classes}, rather than the test classpath — dependencies come through as exploded
 * {@code target/classes} directories on a bare {@code mvn test}, and {@link ModuleFinder} would name
 * every one of them {@code classes}.
 */
class ModulePathResolutionTest {

    private static final Path MODULE_PATH =
            Paths.get(System.getProperty("ringos.module.path", "target/module-path"));

    private static Set<ModuleReference> modules() {
        assertThat(MODULE_PATH)
                .as("the module path directory should have been filled at process-test-classes")
                .exists();
        Set<ModuleReference> found = ModuleFinder.of(MODULE_PATH).findAll();
        // an empty module path resolves perfectly and proves nothing, so make that a failure
        assertThat(found)
                .as("no ringos jars found in %s, so this test would pass vacuously", MODULE_PATH)
                .hasSizeGreaterThanOrEqualTo(5);
        return found;
    }

    /**
     * The readable half. {@link Configuration#resolve} names one offending pair and stops, so work the
     * whole set out first and report every clash at once.
     */
    @Test
    void everyPackageBelongsToExactlyOneModule() {
        Map<String, Set<String>> owners = new TreeMap<>();
        for (ModuleReference module : modules()) {
            for (String pkg : module.descriptor().packages()) {
                owners.computeIfAbsent(pkg, p -> new TreeSet<>()).add(module.descriptor().name());
            }
        }

        Map<String, Set<String>> split = owners.entrySet().stream()
                .filter(e -> e.getValue().size() > 1)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, TreeMap::new));

        assertThat(split)
                .as("a package carried by more than one jar cannot be used on the module path")
                .isEmpty();
    }

    /**
     * The real half: the check the JVM itself makes when it builds the boot layer.
     */
    @Test
    void theWholeSetResolvesAsModules() {
        ModuleFinder finder = ModuleFinder.of(MODULE_PATH);
        Set<String> roots = modules().stream()
                .map(m -> m.descriptor().name())
                .collect(Collectors.toSet());

        assertThatCode(() -> Configuration.resolve(
                finder, java.util.List.of(ModuleLayer.boot().configuration()), ModuleFinder.of(), roots))
                .doesNotThrowAnyException();
    }

    /**
     * Every jar must carry an explicit {@code Automatic-Module-Name}. Five of them cannot derive one at
     * all — the version-stripping heuristic leaves {@code ringos.lib.impl.unsafe.11} and
     * {@code ringos.unsafe.operations.impl.25.provider}, whose digit-leading components are not valid
     * identifiers, so {@link ModuleFinder} refuses the jar outright. The rest could derive one, but it
     * would come from the file name, and renaming an artifact would then silently rename the module
     * consumers wrote {@code requires} against.
     */
    @Test
    void everyJarDeclaresItsOwnModuleName() {
        assertThat(modules()).extracting(m -> m.descriptor().name())
                .as("a module still named after its jar, or one that forgot to set the property")
                .allSatisfy(name -> assertThat(name).startsWith("org.lolaf.ringos").doesNotContain("NOT.SET"));
    }

    /**
     * Guards the reading of the directory itself: a typo in the plugin configuration would leave it
     * empty, and every test above would then pass without checking anything.
     */
    @Test
    void theModulePathHoldsTheRingosJars() throws Exception {
        try (var entries = Files.list(MODULE_PATH)) {
            assertThat(entries.map(p -> p.getFileName().toString()))
                    .allMatch(name -> name.startsWith("ringos-") && name.endsWith(".jar"));
        }
        assertThat(modules()).extracting(m -> m.descriptor().name())
                .contains("org.lolaf.ringos", "org.lolaf.ringos.unsafe", "org.lolaf.ringos.rb.unsafe11");
    }
}
