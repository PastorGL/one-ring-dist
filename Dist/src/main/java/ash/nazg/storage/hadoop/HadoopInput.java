/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.hadoop;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.dist.CSVRecordInputStream;
import ash.nazg.dist.ParquetRecordInputStream;
import ash.nazg.storage.InputAdapter;
import ash.nazg.storage.StorageAdapter;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

public class HadoopInput extends InputAdapter {
    private int partCount;
    private String[] sinkSchema;
    private String[] sinkColumns;
    private char sinkDelimiter;
    private int maxRecordSize;

    private static final int DEFAULT_SIZE = 1024 * 1024;

    @Description("Default Storage that utilizes Hadoop filesystems")
    public Pattern proto() {
        return StorageAdapter.PATH_PATTERN;
    }

    @Override
    protected void configure() throws InvalidConfigValueException {
        partCount = dsResolver.inputParts(name);

        sinkSchema = dsResolver.sinkSchema(name);
        sinkColumns = dsResolver.rawInputColumns(name);
        sinkDelimiter = dsResolver.inputDelimiter(name);

        maxRecordSize = Integer.parseInt(inputResolver.get("max.record.size", String.valueOf(DEFAULT_SIZE)));
    }

    @Override
    public JavaRDD load(String globPattern) {
        // path, regex
        List<Tuple2<String, String>> splits = FileStorage.srcDestGroup(globPattern);

        int executors = Integer.parseInt(context.getConf().get("spark.executor.instances", "-1"));
        int numOfExecutors = (executors <= 0) ? 1 : (int) Math.ceil(executors * 0.8);
        numOfExecutors = Math.max(numOfExecutors, 1);

        // files
        List<String> discoveredFiles = context.parallelize(splits, numOfExecutors)
                .flatMap(srcDestGroup -> {
                    List<String> files = new ArrayList<>();
                    try {
                        Path srcPath = new Path(srcDestGroup._1());

                        Configuration conf = new Configuration();

                        FileSystem srcFS = srcPath.getFileSystem(conf);
                        RemoteIterator<LocatedFileStatus> srcFiles = srcFS.listFiles(srcPath, true);

                        Pattern pattern = Pattern.compile(srcDestGroup._2());

                        while (srcFiles.hasNext()) {
                            String srcFile = srcFiles.next().getPath().toString();

                            Matcher m = pattern.matcher(srcFile);
                            if (m.matches()) {
                                files.add(srcFile);
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Exception while enumerating files to copy: " + e.getMessage());
                        e.printStackTrace(System.err);
                        System.exit(13);
                    }

                    return files.iterator();
                })
                .collect();

        if (partCount <= 0) {
            partCount = numOfExecutors;
        }

        int countOfFiles = discoveredFiles.size();

        int groupSize = countOfFiles / partCount;
        if (groupSize <= 0) {
            groupSize = 1;
        }

        List<List<String>> sinkParts = Lists.partition(discoveredFiles, groupSize);

        final String[] _schema = sinkSchema;
        final String[] _columns = sinkColumns;
        final char _delimiter = sinkDelimiter;
        final int _bufferSize = maxRecordSize;

        return context.parallelize(sinkParts, sinkParts.size())
                .flatMap(src -> {
                    ArrayList<Object> ret = new ArrayList<>();

                    Configuration conf = new Configuration();
                    try {
                        for (String inputFile : src) {
                            Path inputFilePath = new Path(inputFile);
                            InputStream inputStream;

                            String suffix = FileStorage.suffix(inputFilePath);

                            if ("parquet".equalsIgnoreCase(suffix)) {
                                ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, inputFilePath, ParquetMetadataConverter.NO_FILTER);
                                MessageType schema = readFooter.getFileMetaData().getSchema();

                                int[] fieldOrder;
                                if (_columns != null) {
                                    fieldOrder = new int[_columns.length];

                                    for (int i = 0; i < _columns.length; i++) {
                                        String column = _columns[i];
                                        fieldOrder[i] = schema.getFieldIndex(column);
                                    }
                                } else {
                                    fieldOrder = IntStream.range(0, schema.getFieldCount()).toArray();
                                }

                                GroupReadSupport readSupport = new GroupReadSupport();
                                readSupport.init(conf, null, schema);
                                ParquetReader<Group> reader = ParquetReader.builder(readSupport, inputFilePath).build();

                                inputStream = new ParquetRecordInputStream(reader, fieldOrder, _delimiter);
                            } else {
                                FileSystem inputFs = inputFilePath.getFileSystem(conf);
                                inputStream = inputFs.open(inputFilePath);

                                suffix = suffix.toLowerCase();
                                if (FileStorage.CODECS.containsKey(suffix)) {
                                    Class<? extends CompressionCodec> cc = FileStorage.CODECS.get(suffix);
                                    CompressionCodec codec = cc.newInstance();
                                    ((Configurable) codec).setConf(conf);

                                    inputStream = codec.createInputStream(inputStream);
                                }

                                if ((_schema != null) || (_columns != null)) {
                                    int[] columnOrder;

                                    if (_schema != null) {
                                        if (_columns == null) {
                                            columnOrder = IntStream.range(0, _schema.length).toArray();
                                        } else {
                                            Map<String, Integer> schema = new HashMap<>();
                                            for (int i = 0; i < _schema.length; i++) {
                                                schema.put(_schema[i], i);
                                            }

                                            Map<Integer, String> columns = new HashMap<>();
                                            for (int i = 0; i < _columns.length; i++) {
                                                columns.put(i, _columns[i]);
                                            }

                                            columnOrder = new int[_columns.length];
                                            for (int i = 0; i < _columns.length; i++) {
                                                columnOrder[i] = schema.get(columns.get(i));
                                            }
                                        }
                                    } else {
                                        columnOrder = IntStream.range(0, _columns.length).toArray();
                                    }

                                    inputStream = new CSVRecordInputStream(inputStream, columnOrder, _delimiter);
                                }
                            }

                            int len;
                            for (byte[] buffer = new byte[_bufferSize]; (len = inputStream.read(buffer)) > 0; ) {
                                String str = new String(buffer, 0, len, StandardCharsets.UTF_8);
                                ret.add(str);
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Exception while reading records: " + e.getMessage());
                        e.printStackTrace(System.err);
                        System.exit(14);
                    }

                    return ret.iterator();
                });
    }
}
