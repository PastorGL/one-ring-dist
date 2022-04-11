/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage;

import ash.nazg.config.tdl.*;
import ash.nazg.scripting.*;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class StorageTestRunner implements AutoCloseable {
    private final JavaSparkContext context;
    private final ScriptHolder script;

    public StorageTestRunner(boolean replpath, String path) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("One Ring Test Runner")
                .set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getCanonicalName())
                .setMaster("local[*]")
                .set("spark.network.timeout", "10000")
                .set("spark.ui.enabled", "false");

        context = new JavaSparkContext(sparkConf);
        context.hadoopConfiguration().set(FileInputFormat.INPUT_DIR_RECURSIVE, Boolean.TRUE.toString());

        try (InputStream input = getClass().getResourceAsStream(path)) {
            Properties source = new Properties();
            source.load(input);

            if (replpath) {
                String rootResourcePath = getClass().getResource("/").getPath();
                for (Object p : source.keySet()) {
                    String prop = (String) p;
                    if (prop.startsWith(Constants.INPUT_LAYER + "." + Constants.PATH_PREFIX)) {
                        source.setProperty(prop, rootResourcePath + source.get(p));
                    }
                }
            }

            script = new ScriptHolder(IOUtils.toString(input, StandardCharsets.UTF_8), null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, JavaRDDLike> go() throws Exception {
        DataContext streams = new DataContext(context);
        for (String input : script.input) {
            String path = ioResolver.inputPath(input);

            InputAdapter inputAdapter = Adapters.inputAdapter(path);
            inputAdapter.initialize(context);
            inputAdapter.configure(input, script);
            streams.put(input, inputAdapter.load(path));
        }

        TDL4Interpreter tdl4 = new TDL4Interpreter(script);
        tdl4.initialize(streams);
        tdl4.interpret();

        Map<String, JavaRDDLike> result = tdl4.result();

        Set<String> rddNames = result.keySet();
        Set<String> outputNames = new HashSet<>();
        for (String output : script.output) {
            for (String name : rddNames) {
                if (name.equals(output)) {
                    outputNames.add(name);
                }
            }
        }

        for (String output : outputNames) {
            JavaRDDLike rdd = result.get(output);

            if (rdd != null) {
                String path = ioResolver.outputPath(output);

                OutputAdapter outputAdapter = Adapters.outputAdapter(path);
                outputAdapter.initialize(context);
                outputAdapter.configure(output, script);
                outputAdapter.save(path, (JavaRDD) rdd);
            }
        }

        return result;
    }

    public void close() {
        context.stop();
    }
}
