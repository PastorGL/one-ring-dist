/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage;

import ash.nazg.dist.InvalidConfigurationException;
import ash.nazg.metadata.AdapterMeta;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Map;

public abstract class StorageAdapter {
    public final AdapterMeta meta;

    protected JavaSparkContext context;
    protected String dsName;

    public StorageAdapter() {
        this.meta = meta();
    }

    public void initialize(JavaSparkContext ctx) {
        this.context = ctx;
    }

    protected void configure(String name, Map adapterConfig) throws InvalidConfigurationException {
        this.dsName = name;

        configure();
    }

    protected abstract AdapterMeta meta();

    abstract protected void configure() throws InvalidConfigurationException;
}
