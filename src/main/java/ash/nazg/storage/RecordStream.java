package ash.nazg.storage;

import ash.nazg.data.BinRec;

import java.io.IOException;

public interface RecordStream extends AutoCloseable {
    BinRec ensureRecord() throws IOException;
}
