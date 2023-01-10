package ash.nazg.storage.hadoop;

import ash.nazg.data.BinRec;
import ash.nazg.storage.RecordStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class PlainTextRecordStream implements RecordStream {
    private final BufferedReader reader;

    public PlainTextRecordStream(InputStream inputStream) {
        this.reader = new BufferedReader(new InputStreamReader(inputStream));
    }

    @Override
    public BinRec ensureRecord() throws IOException {
        String line = reader.readLine();

        if (line == null) {
            return null;
        }

        return new BinRec(line.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close() throws Exception {
        reader.close();
    }
}
