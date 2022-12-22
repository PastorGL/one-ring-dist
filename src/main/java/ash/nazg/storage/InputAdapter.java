/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage;

import ash.nazg.dist.InvalidConfigurationException;
import ash.nazg.metadata.AdapterResolver;
import org.apache.spark.api.java.JavaRDDLike;

import java.util.Map;

public abstract class InputAdapter extends StorageAdapter {
    public void configure(Map<String, Object> adapterConfig) throws InvalidConfigurationException {
        resolver = new AdapterResolver(meta, adapterConfig);

        configure();
    }

    public abstract Map<String, JavaRDDLike> load(String path) throws Exception;
}
