/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.dist;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.TaskWrapperConfigBuilder;
import ash.nazg.config.tdl.Constants;
import ash.nazg.config.tdl.LayerResolver;
import ash.nazg.config.tdl.StreamResolver;
import ash.nazg.config.tdl.TaskDefinitionLanguage;
import ash.nazg.storage.Adapters;
import ash.nazg.storage.InputAdapter;
import ash.nazg.storage.OutputAdapter;
import ash.nazg.storage.hadoop.HadoopInput;
import ash.nazg.storage.hadoop.HadoopOutput;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Main {
    private static final Logger LOG = Logger.getLogger(Main.class);

    public static void main(String[] args) {
        TaskWrapperConfigBuilder configBuilder = new TaskWrapperConfigBuilder();
        configBuilder.addRequiredOption("c", "config", true, "Config file");
        configBuilder.addRequiredOption("t", "tmpDir", true, "Location for temporary files");
        configBuilder.addOption("d", "direction", true, "Copy direction. Can be 'from', 'to', or 'nop' to just validate the config file and exit");

        JavaSparkContext context = null;
        try {
            configBuilder.setCommandLine(args);

            SparkConf sparkConf = new SparkConf()
                    .setAppName("One Ring Dist")
                    .set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getCanonicalName());

            boolean local = configBuilder.hasOption("local");
            if (local) {
                String cores = "*";
                if (configBuilder.hasOption("localCores")) {
                    cores = configBuilder.getOptionValue("localCores");
                }

                sparkConf
                        .setMaster("local[" + cores + "]")
                        .set("spark.network.timeout", "10000");

                if (configBuilder.hasOption("driverMemory")) {
                    sparkConf.set("spark.driver.memory", configBuilder.getOptionValue("driverMemory"));
                }
                sparkConf.set("spark.ui.enabled", String.valueOf(configBuilder.hasOption("sparkUI")));
            }

            context = new JavaSparkContext(sparkConf);
            context.hadoopConfiguration().set(FileInputFormat.INPUT_DIR_RECURSIVE, Boolean.TRUE.toString());

            TaskDefinitionLanguage.Task taskConfig = configBuilder.build(context, local);
            configBuilder.foreignLayerVariable(taskConfig, "dist.wrap", "d");
            configBuilder.foreignLayerVariable(taskConfig, "dist.store", "S");
            configBuilder.foreignLayerVariable(taskConfig, "dist.tmp", "t");

            TaskDefinitionLanguage.Definitions props = taskConfig.foreignLayer(Constants.DIST_LAYER);
            if (!props.containsKey("tmp")) {
                props.put("tmp", local ? System.getProperty("java.io.tmpdir") : "hdfs:///tmp");
            }
            LayerResolver distResolver = new LayerResolver(props);

            String wrapperStorePath = distResolver.get("store");
            if (!local && (wrapperStorePath == null)) {
                throw new InvalidConfigValueException("An invocation on the cluster must have wrapper store path set");
            }

            Direction direction = Direction.parse(distResolver.get("wrap", "nop"));
            if (direction.anyDirection) {
                StreamResolver dsResolver = new StreamResolver(taskConfig.dataStreams);

                List<Tuple3<String, String, String>> paths = new ArrayList<>();

                if (direction.toCluster) {
                    for (String sink : taskConfig.sink) {
                        paths.add(new Tuple3<>(sink, dsResolver.inputPath(sink), dsResolver.inputPath(Constants.DEFAULT_DS) + "/" + sink));
                    }
                }

                if (direction.fromCluster) {
                    if (wrapperStorePath != null) {
                        final char _delimiter = dsResolver.inputDelimiter(Constants.DEFAULT_DS);

                        Map<String, String> wrapperStore = context.textFile(wrapperStorePath + "/outputs/part-00000")
                                .mapPartitionsToPair(it -> {
                                    List<Tuple2<String, String>> ret = new ArrayList<>();

                                    CSVParser parser = new CSVParserBuilder().withSeparator(_delimiter).build();
                                    while (it.hasNext()) {
                                        String l = it.next();

                                        String[] row = parser.parseLine(l);
                                        ret.add(new Tuple2<>(row[0], row[1]));
                                    }

                                    return ret.iterator();
                                })
                                .collectAsMap();

                        for (Map.Entry<String, String> entry : wrapperStore.entrySet()) {
                            String tee = entry.getKey();
                            paths.add(new Tuple3<>(tee, entry.getValue(), dsResolver.outputPath(tee)));
                        }
                    } else {
                        for (String tee : taskConfig.tees) {
                            paths.add(new Tuple3<>(tee, dsResolver.outputPath(Constants.DEFAULT_DS) + "/" + tee, dsResolver.outputPath(tee)));
                        }
                    }
                }

                for (Tuple3<String, String, String> pathEntry : paths) {
                    String ds = pathEntry._1();
                    String pathFrom = pathEntry._2();
                    String pathTo = pathEntry._3();

                    Class<? extends InputAdapter> inputClass = Adapters.input(pathFrom);
                    InputAdapter inputAdapter = (inputClass == null) ? new HadoopInput() : inputClass.newInstance();
                    inputAdapter.initialize(context);
                    inputAdapter.configure(ds, taskConfig);
                    JavaRDD rdd = inputAdapter.load(pathFrom);

                    Class<? extends OutputAdapter> outputClass = Adapters.output(pathTo);
                    OutputAdapter outputAdapter = (outputClass == null) ? new HadoopOutput() : outputClass.newInstance();
                    outputAdapter.initialize(context);
                    outputAdapter.configure(ds, taskConfig);
                    outputAdapter.save(pathTo, rdd);
                }
            }
        } catch (Exception ex) {
            if (ex instanceof ParseException) {
                new HelpFormatter().printHelp("One Ring Dist", configBuilder.getOptions());
            } else {
                LOG.error(ex.getMessage(), ex);
            }

            System.exit(1);
        } finally {
            if (context != null) {
                context.stop();
            }
        }
    }
}
