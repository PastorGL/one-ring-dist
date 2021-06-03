/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage;

import ash.nazg.config.RegisteredPackages;
import ash.nazg.storage.metadata.AdapterMeta;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Adapters {
    static public final Map<String, AdapterInfo> INPUTS;
    static public final Map<String, AdapterInfo> OUTPUTS;

    static public final Map<String, String> INPUT_PACKAGES;
    static public final Map<String, String> OUTPUT_PACKAGES;

    static {
        Map<String, AdapterInfo> inputs = new HashMap<>();
        Map<String, AdapterInfo> outputs = new HashMap<>();
        Map<String, String> inPackages = new HashMap<>();
        Map<String, String> outPackages = new HashMap<>();

        for (Map.Entry<String, String> pkg : RegisteredPackages.REGISTERED_PACKAGES.entrySet()) {
            try (ScanResult scanResult = new ClassGraph().enableClassInfo().acceptPackages(pkg.getKey()).scan()) {
                ClassInfoList iaClasses = scanResult.getSubclasses(InputAdapter.class.getTypeName());
                List<Class<?>> iaClassRefs = iaClasses.loadClasses();

                for (Class<?> iaClass : iaClassRefs) {
                    try {
                        InputAdapter ia = (InputAdapter) iaClass.newInstance();
                        AdapterMeta meta = ia.meta;
                        AdapterInfo ai = new AdapterInfo((Class<? extends StorageAdapter>) iaClass, meta);
                        inputs.put(meta.name, ai);
                    } catch (Exception e) {
                        System.err.println("Cannot instantiate Input Adapter class '" + iaClass.getTypeName() + "'");
                        e.printStackTrace(System.err);
                        System.exit(-8);
                    }
                }

                if (!iaClassRefs.isEmpty()) {
                    inPackages.put(pkg.getKey(), pkg.getValue());
                }

                ClassInfoList oaClasses = scanResult.getSubclasses(OutputAdapter.class.getTypeName());
                List<Class<?>> oaClassRefs = oaClasses.loadClasses();

                for (Class<?> oaClass : oaClassRefs) {
                    try {
                        OutputAdapter oa = (OutputAdapter) oaClass.newInstance();
                        AdapterMeta meta = oa.meta;
                        AdapterInfo ai = new AdapterInfo((Class<? extends StorageAdapter>) oaClass, meta);
                        outputs.put(meta.name, ai);
                    } catch (Exception e) {
                        System.err.println("Cannot instantiate Output Adapter class '" + oaClass.getTypeName() + "'");
                        e.printStackTrace(System.err);
                        System.exit(-8);
                    }
                }

                if (!oaClassRefs.isEmpty()) {
                    outPackages.put(pkg.getKey(), pkg.getValue());
                }
            }
        }

        INPUTS = Collections.unmodifiableMap(inputs);
        OUTPUTS = Collections.unmodifiableMap(outputs);
        INPUT_PACKAGES = Collections.unmodifiableMap(inPackages);
        OUTPUT_PACKAGES = Collections.unmodifiableMap(outPackages);
    }

    static public Class<? extends InputAdapter> inputClass(String path) {
        for (AdapterInfo ia : INPUTS.values()) {
            if (path.matches(ia.meta.pattern)) {
                return (Class<? extends InputAdapter>) ia.adapterClass;
            }
        }

        return null;
    }

    static public Class<? extends OutputAdapter> outputClass(String path) {
        for (AdapterInfo oa : OUTPUTS.values()) {
            if (path.matches(oa.meta.pattern)) {
                return (Class<? extends OutputAdapter>) oa.adapterClass;
            }
        }

        return null;
    }

    public static Map<String, AdapterInfo> packageInputs(String pkgName) {
        Map<String, AdapterInfo> ret = new HashMap<>();

        for (Map.Entry<String, AdapterInfo> e : INPUTS.entrySet()) {
            if (e.getValue().adapterClass.getPackage().getName().equals(pkgName)) {
                ret.put(e.getKey(), e.getValue());
            }
        }

        return ret;
    }

    public static Map<String, AdapterInfo> packageOutputs(String pkgName) {
        Map<String, AdapterInfo> ret = new HashMap<>();

        for (Map.Entry<String, AdapterInfo> e : OUTPUTS.entrySet()) {
            if (e.getValue().adapterClass.getPackage().getName().equals(pkgName)) {
                ret.put(e.getKey(), e.getValue());
            }
        }

        return ret;
    }
}
