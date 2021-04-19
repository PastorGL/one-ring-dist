/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.hadoop;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.storage.OutputAdapter;
import ash.nazg.storage.StorageAdapter;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

public class HadoopOutput extends OutputAdapter {
    private String codec;
    private String[] columns;
    private char delimiter;

    @Description("Default Storage that utilizes Hadoop filesystems")
    public Pattern proto() {
        return StorageAdapter.PATH_PATTERN;
    }

    protected void configure() throws InvalidConfigValueException {
        codec = outputResolver.get("codec", "none");

        columns = dsResolver.outputColumns(name);
        delimiter = dsResolver.outputDelimiter(name);
    }

    @Override
    public void save(String path, JavaRDD rdd) {
        Path outputPath = new Path(path);

        String suffix = FileStorage.suffix(outputPath);
        if (suffix.equalsIgnoreCase("parquet")) {
            final String _codec = codec;
            final String[] _columns = columns;
            final String _name = name;
            final char _delimiter = delimiter;

            ((JavaRDD<Object>) rdd).mapPartitionsWithIndex((idx, it) -> {
                Configuration conf = new Configuration();
                Path partPath = new Path(outputPath, String.format("%05d", idx)
                        + (!"none".equalsIgnoreCase(codec) ? "." + codec : "")
                        + ".parquet");

                List<Type> types = new ArrayList<>();
                for (String col : _columns) {
                    types.add(Types.primitive(BINARY, Type.Repetition.REQUIRED).as(stringType()).named(col));
                }
                MessageType schema = new MessageType(_name, types);

                ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(partPath)
                        .withConf(conf)
                        .withType(schema)
                        .withPageWriteChecksumEnabled(false);
                if (!"none".equalsIgnoreCase(codec)) {
                    builder.withCompressionCodec(CompressionCodecName.fromCompressionCodec(FileStorage.CODECS.get(_codec)));
                }
                ParquetWriter<Group> writer = builder.build();

                CSVParser parser = new CSVParserBuilder().withSeparator(_delimiter).build();
                int numCols = _columns.length;
                while (it.hasNext()) {
                    String line = String.valueOf(it.hasNext());

                    String[] ll = parser.parseLine(line);
                    Group group = new SimpleGroup(schema);

                    for (int i = 0; i < numCols; i++) {
                        group.add(i, ll[i]);
                    }

                    writer.write(group);
                }

                writer.close();
                return Collections.emptyIterator();
            }, true).count();
        } else {
            if (FileStorage.CODECS.containsKey(codec)) {
                rdd.saveAsTextFile(path + "." + codec, FileStorage.CODECS.get(codec));
            } else {
                rdd.saveAsTextFile(path);
            }
        }
    }
}
