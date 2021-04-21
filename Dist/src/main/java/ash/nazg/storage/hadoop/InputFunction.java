package ash.nazg.storage.hadoop;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.IntStream;

public class InputFunction implements FlatMapFunction<List<String>, Object> {
    final protected String[] _schema;
    final protected String[] _columns;
    final protected char _delimiter;
    final protected int _bufferSize;

    public InputFunction(String[] schema, String[] columns, char delimiter, int bufferSize) {
        _schema = schema;
        _columns = columns;
        _delimiter = delimiter;
        _bufferSize = bufferSize;
    }

    @Override
    public Iterator<Object> call(List<String> src) {
            ArrayList<Object> ret = new ArrayList<>();

            Configuration conf = new Configuration();
            try {
                for (String inputFile : src) {
                    Path inputFilePath = new Path(inputFile);
                    InputStream inputStream = decorateInputStream(conf, inputFilePath);

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
    }

    private InputStream decorateInputStream(Configuration conf, Path inputFilePath) throws IOException, InstantiationException, IllegalAccessException {
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

        return inputStream;
    }
}
