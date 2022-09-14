/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage;

import ash.nazg.dist.InvalidConfigurationException;
import ash.nazg.metadata.AdapterResolver;
import org.apache.spark.api.java.JavaRDDLike;

import java.util.Map;

public abstract class OutputAdapter extends StorageAdapter {
    protected AdapterResolver outputResolver;

    public void configure(String name, Map adapterConfig) throws InvalidConfigurationException {
        outputResolver = new AdapterResolver(meta, adapterConfig);

        super.configure(name, adapterConfig);
    }

    public abstract void save(String path, JavaRDDLike rdd);
}
