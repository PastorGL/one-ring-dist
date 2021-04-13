/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage;

import ash.nazg.config.Packages;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Adapters {

    static private final List<AdapterInfo> INPUT_ADAPTERS = new ArrayList<>();
    static private final List<AdapterInfo> OUTPUT_ADAPTERS = new ArrayList<>();
    static private AdapterInfo fallbackInput = null;
    static private AdapterInfo fallbackOutput = null;

    static {
        try (ScanResult scanResult = new ClassGraph()
                .enableClassInfo()
                .acceptPackages(Packages.getRegisteredPackages().keySet().toArray(new String[0]))
                .scan()) {

            ClassInfoList iaClasses = scanResult.getSubclasses(InputAdapter.class.getTypeName());
            List<Class<?>> iaClassRefs = iaClasses.loadClasses();

            for (Class<?> iaClass : iaClassRefs) {
                try {
                    InputAdapter ia = (InputAdapter) iaClass.newInstance();
                    AdapterInfo ai = new AdapterInfo(ia.proto(), (Class<? extends StorageAdapter>) iaClass);
                    if (ia instanceof HadoopInput) {
                        fallbackInput = ai;
                    } else {
                        INPUT_ADAPTERS.add(ai);
                    }
                } catch (Exception e) {
                    System.err.println("Cannot instantiate Input Adapter class '" + iaClass.getTypeName() + "'");
                    e.printStackTrace(System.err);
                    System.exit(-8);
                }
            }
        }

        if (fallbackInput != null) {
            INPUT_ADAPTERS.add(fallbackInput);
        } else {
            System.err.println("There is no fallback Input Adapter in the classpath. Won't continue");
            System.exit(-8);
        }

        try (ScanResult scanResult = new ClassGraph()
                .enableClassInfo()
                .acceptPackages(Packages.getRegisteredPackages().keySet().toArray(new String[0]))
                .scan()) {

            ClassInfoList oaClasses = scanResult.getSubclasses(OutputAdapter.class.getTypeName());
            List<Class<?>> oaClassRefs = oaClasses.loadClasses();

            for (Class<?> oaClass : oaClassRefs) {
                try {
                    OutputAdapter oa = (OutputAdapter) oaClass.newInstance();
                    AdapterInfo ai = new AdapterInfo(oa.proto(), (Class<? extends StorageAdapter>) oaClass);
                    if (oa instanceof HadoopOutput) {
                        fallbackOutput = ai;
                    } else {
                        OUTPUT_ADAPTERS.add(ai);
                    }
                } catch (Exception e) {
                    System.err.println("Cannot instantiate Output Adapter class '" + oaClass.getTypeName() + "'");
                    e.printStackTrace(System.err);
                    System.exit(-8);
                }
            }
        }

        if (fallbackOutput != null) {
            OUTPUT_ADAPTERS.add(fallbackOutput);
        } else {
            System.err.println("There is no fallback Output Adapter in the classpath. Won't continue");
            System.exit(-8);
        }
    }

    static public Map<String, AdapterInfo> getAvailableInputAdapters(String pkgName) {
        Map<String, AdapterInfo> ret = new HashMap<>();

        INPUT_ADAPTERS.forEach(e -> {
            if (e.getClass().getPackage().getName().equals(pkgName)) {
                ret.put(e.getClass().getSimpleName(), e);
            }
        });

        return ret;
    }

    static public Map<String, AdapterInfo> getAvailableOutputAdapters(String pkgName) {
        Map<String, AdapterInfo> ret = new HashMap<>();

        OUTPUT_ADAPTERS.forEach(e -> {
            if (e.getClass().getPackage().getName().equals(pkgName)) {
                ret.put(e.getClass().getSimpleName(), e);
            }
        });

        return ret;
    }

    static public AdapterInfo getInputAdapter(String name) {
        return INPUT_ADAPTERS.stream().filter(e -> e.getClass().getSimpleName().equals(name)).findFirst().orElse(null);
    }

    static public AdapterInfo getOutputAdapter(String name) {
        return OUTPUT_ADAPTERS.stream().filter(e -> e.getClass().getSimpleName().equals(name)).findFirst().orElse(null);
    }

    static public Class<? extends InputAdapter> input(String path) {
        for (AdapterInfo ia : INPUT_ADAPTERS) {
            if (ia.proto.matcher(path).matches()) {
                return (Class<? extends InputAdapter>) ia.adapterClass;
            }
        }

        return (Class<? extends InputAdapter>) fallbackInput.adapterClass;
    }

    static public Class<? extends OutputAdapter> output(String path) {
        for (AdapterInfo oa : OUTPUT_ADAPTERS) {
            if (oa.proto.matcher(path).matches()) {
                return (Class<? extends OutputAdapter>) oa.adapterClass;
            }
        }

        return (Class<? extends OutputAdapter>) fallbackOutput.adapterClass;
    }
}
