/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.hadoop;

import ash.nazg.data.BinRec;
import ash.nazg.storage.RecordStream;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class DelimitedTextRecordStream implements RecordStream {
    private final int[] order;
    private final BufferedReader reader;
    private final CSVParser parser;
    private final List<String> columns;

    public DelimitedTextRecordStream(InputStream input, char delimiter, String[] _schema, String[] _columns) {
        int[] columnOrder;

        if (_schema != null) {
            if (_columns == null) {
                columnOrder = IntStream.range(0, _schema.length).toArray();
                _columns = _schema;
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

        this.columns = Arrays.asList(_columns);
        this.order = columnOrder;
        this.reader = new BufferedReader(new InputStreamReader(input));
        this.parser = new CSVParserBuilder().withSeparator(delimiter).build();
    }

    public BinRec ensureRecord() throws IOException {
        String line = reader.readLine();

        if (line == null) {
            return null;
        }

        try {
            String[] ll = parser.parseLine(line);
            String[] acc = new String[order.length];

            for (int i = 0; i < order.length; i++) {
                int l = order[i];
                acc[i] = ll[l];
            }

            return new BinRec(columns, acc);
        } catch (Exception e) {
            throw new IOException("Malformed input line: " + line, e);
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
