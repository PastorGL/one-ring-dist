/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.hadoop;

import ash.nazg.dist.InvalidConfigurationException;
import ash.nazg.metadata.DefinitionMetaBuilder;
import ash.nazg.storage.OutputAdapter;
import ash.nazg.metadata.AdapterMeta;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.Function2;

import java.util.Iterator;

public class HadoopOutput extends OutputAdapter {
    protected static final String CODEC = "codec";
    protected static final String COLUMNS = "columns";
    protected static final String DELIMITER = "delimiter";

    protected HadoopStorage.Codec codec;
    protected String[] columns;
    protected char delimiter;

    @Override
    protected AdapterMeta meta() {
        return new AdapterMeta("hadoop", "Default output adapter that utilizes Hadoop FileSystems." +
                " Supports text, text-based columnar (CSV/TSV), and Parquet files, optionally compressed",

                new DefinitionMetaBuilder()
                        .def(CODEC, "Codec to compress the output", HadoopStorage.Codec.class, HadoopStorage.Codec.NONE,
                                "By default, use no compression")
                        .def(COLUMNS, "Columns to write",
                                String[].class, null, "By default, select all columns")
                        .def(DELIMITER, "Record column delimiter",
                                String.class, "\t", "By default, tabulation character")
                        .build()
        );
    }

    protected void configure() throws InvalidConfigurationException {
        codec = resolver.get(CODEC);

        columns = resolver.get(COLUMNS);
        delimiter = resolver.get(DELIMITER);
    }

    @Override
    public void save(String path, JavaRDDLike rdd) {
        Function2<Integer, Iterator<Text>, Iterator<Void>> outputFunction = new PartOutputFunction(dsName, path, codec, columns, delimiter);

        rdd.mapPartitionsWithIndex(outputFunction, true).count();
    }
}
