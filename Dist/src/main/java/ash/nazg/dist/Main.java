/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.dist;

import ash.nazg.config.TaskWrapperConfigBuilder;
import ash.nazg.config.tdl.*;
import ash.nazg.storage.Adapters;
import ash.nazg.storage.InputAdapter;
import ash.nazg.storage.OutputAdapter;
import ash.nazg.storage.hadoop.HadoopInput;
import ash.nazg.storage.hadoop.HadoopOutput;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;

public class Main {
    private static final Logger LOG = Logger.getLogger(Main.class);

    public static void main(String[] args) {
        TaskWrapperConfigBuilder configBuilder = new TaskWrapperConfigBuilder();
        configBuilder.addOption("t", "tmpDir", true, "Location for temporary files");
        configBuilder.addRequiredOption("d", "direction", true, "Copy direction. Can be 'from', 'to', or 'nop' to just validate the config file and exit");

        JavaSparkContext context = null;
        try {
            configBuilder.setCommandLine(args, "Dist");

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

            TaskDefinitionLanguage.Task config = configBuilder.build(context);
            configBuilder.foreignLayerVariable(config, "dist.tmp", "t");

            TaskDefinitionLanguage.Definitions props = config.foreignLayer(Constants.DIST_LAYER);
            if (!props.containsKey("tmp")) {
                props.put("tmp", local ? System.getProperty("java.io.tmpdir") : "hdfs:///tmp");
            }
            LayerResolver distResolver = new LayerResolver(props);

            Direction taskDirection = Direction.parse(distResolver.get("wrap", "nop"));
            Direction distDirection = Direction.parse(configBuilder.getOptionValue("d"));
            if (taskDirection.anyDirection && distDirection.anyDirection) {
                InOutResolver ioResolver = new InOutResolver(config);

                List<Tuple3<String, String, String>> paths = new ArrayList<>();

                if (taskDirection.toCluster && distDirection.toCluster) {
                    for (String input : config.input) {
                        String pathFrom = ioResolver.inputPath(input);
                        String pathTo = ioResolver.inputPathNonLocal(input);
                        if (!pathFrom.equals(pathTo)) {
                            paths.add(new Tuple3<>(input, pathFrom, pathTo));
                        }
                    }
                }

                if (taskDirection.fromCluster && distDirection.fromCluster) {
                    String wrapperStorePath = distResolver.get("store");

                    if (wrapperStorePath != null) {
                        StreamResolver dsResolver = new StreamResolver(config.dataStreams);
                        List<String> wrapperStore = context.textFile(wrapperStorePath + "/outputs/part-00000")
                                .collect();

                        final char outputsDelimiter = dsResolver.outputDelimiter(Constants.OUTPUTS_DS);
                        CSVParser parser = new CSVParserBuilder().withSeparator(outputsDelimiter).build();
                        for (String line : wrapperStore) {
                            final String[] _output = parser.parseLine(String.valueOf(line));

                            String pathFrom = ioResolver.outputPathNonLocal(_output[0]) + "/*";
                            String pathTo = ioResolver.outputPath(_output[0]);
                            if (!pathTo.equals(pathFrom)) {
                                paths.add(new Tuple3<>(_output[0], pathFrom, pathTo));
                                config.dataStreams.compute(_output[0], (k, ds) -> {
                                    if (ds == null) {
                                        ds = new TaskDefinitionLanguage.DataStream();
                                    }
                                    ds.input.partCount = _output[1];
                                    return ds;
                                });
                            }
                        }
                    } else {
                        for (String output : config.output) {
                            String pathFrom = ioResolver.outputPathNonLocal(output) + "/*";
                            String pathTo = ioResolver.outputPath(output);
                            if (!pathTo.equals(pathFrom)) {
                                paths.add(new Tuple3<>(output, pathFrom, pathTo));
                            }
                        }
                    }
                }

                for (Tuple3<String, String, String> pathEntry : paths) {
                    String dsName = pathEntry._1();
                    String pathFrom = pathEntry._2();
                    String pathTo = pathEntry._3();

                    Class<? extends InputAdapter> inputClass = Adapters.inputClass(pathFrom);
                    InputAdapter inputAdapter = (inputClass == null) ? new HadoopInput() : inputClass.newInstance();
                    inputAdapter.initialize(context);
                    inputAdapter.configure(dsName, config);
                    JavaRDD rdd = inputAdapter.load(pathFrom);

                    Class<? extends OutputAdapter> outputClass = Adapters.outputClass(pathTo);
                    OutputAdapter outputAdapter = (outputClass == null) ? new HadoopOutput() : outputClass.newInstance();
                    outputAdapter.initialize(context);
                    outputAdapter.configure(dsName, config);
                    outputAdapter.save(pathTo, rdd);
                }
            }
        } catch (Exception ex) {
            if (ex instanceof ParseException) {
                configBuilder.printHelp("Dist");
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
