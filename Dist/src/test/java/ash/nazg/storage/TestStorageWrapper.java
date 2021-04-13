/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage;

import ash.nazg.config.tdl.Constants;
import ash.nazg.config.tdl.Interpreter;
import ash.nazg.config.tdl.PropertiesReader;
import ash.nazg.config.tdl.StreamResolver;
import ash.nazg.spark.WrapperBase;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.InputStream;
import java.util.*;

public class TestStorageWrapper extends WrapperBase implements AutoCloseable {
    public final Map<String, JavaRDDLike> result = new HashMap<>();

    private static final SparkConf sparkConf = new SparkConf()
            .setAppName("test")
            .set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getCanonicalName())
            .setMaster("local[*]")
            .set("spark.network.timeout", "10000")
            .set("spark.ui.enabled", "false");

    public TestStorageWrapper(boolean replpath, String path) {
        context.hadoopConfiguration().set(FileInputFormat.INPUT_DIR_RECURSIVE, Boolean.TRUE.toString());

        try (InputStream input = getClass().getResourceAsStream(path)) {
            Properties source = new Properties();
            source.load(input);

            if (replpath) {
                String rootResourcePath = getClass().getResource("/").getPath();
                for (Object p : source.keySet()) {
                    String prop = (String) p;
                    if (prop.startsWith(Constants.DS_INPUT_PATH_PREFIX)) {
                        source.setProperty(prop, rootResourcePath + source.get(p));
                    }
                }
            }

            configure(new JavaSparkContext(sparkConf), PropertiesReader.toTask(source, null));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public void close() {
        context.stop();
    }

    public void go() throws Exception {
        StreamResolver dsResolver = new StreamResolver(taskConfig.dataStreams);

        for (String sink : taskConfig.sink) {
            String path = dsResolver.inputPath(sink);

            InputAdapter inputAdapter = Adapters.input(path).newInstance();
            inputAdapter.initialize(context);
            inputAdapter.configure(sink, taskConfig.dataStreams);
            result.put(sink, inputAdapter.load(path));
        }

        new Interpreter(taskConfig, context).processTaskChain(result);

        Set<String> rddNames = result.keySet();
        Set<String> teeNames = new HashSet<>();
        for (String tee : taskConfig.tees) {
            for (String name : rddNames) {
                if (name.equals(tee)) {
                    teeNames.add(name);
                }
            }
        }

        for (String teeName : teeNames) {
            JavaRDDLike rdd = result.get(teeName);

            if (rdd != null) {
                String path = dsResolver.outputPath(teeName);

                OutputAdapter outputAdapter = Adapters.output(path).newInstance();
                outputAdapter.initialize(context);
                outputAdapter.configure(teeName, taskConfig.dataStreams);
                outputAdapter.save(path, rdd);
            }
        }
    }
}
