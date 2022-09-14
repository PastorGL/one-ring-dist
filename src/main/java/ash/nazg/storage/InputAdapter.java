/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage;

import ash.nazg.dist.InvalidConfigurationException;
import ash.nazg.metadata.AdapterResolver;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;

import java.util.Map;

public abstract class InputAdapter extends StorageAdapter {
    protected AdapterResolver inputResolver;

    public abstract JavaRDD<Text> load(String path) throws Exception;

    public void configure(String name, Map adapterConfig) throws InvalidConfigurationException {
        inputResolver = new AdapterResolver(meta, adapterConfig);

        super.configure(name, adapterConfig);
    }
}
