/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage;

import ash.nazg.dist.InvalidConfigurationException;
import ash.nazg.metadata.AdapterMeta;
import ash.nazg.metadata.AdapterResolver;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Map;

public abstract class StorageAdapter {
    public final AdapterMeta meta;

    protected JavaSparkContext context;

    protected AdapterResolver resolver;

    public StorageAdapter() {
        this.meta = meta();
    }

    public void initialize(JavaSparkContext ctx) {
        this.context = ctx;
    }

    protected abstract AdapterMeta meta();

    abstract protected void configure() throws InvalidConfigurationException;
}
