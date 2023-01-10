package ash.nazg.metadata;

import ash.nazg.data.BinRec;
import org.apache.spark.api.java.JavaRDD;

public class DataHolder {
    public final JavaRDD<BinRec> underlyingRdd;
    public final String sub;

    public DataHolder(JavaRDD<BinRec> underlyingRdd, String sub) {
        this.underlyingRdd = underlyingRdd;
        this.sub = sub;
    }
}
