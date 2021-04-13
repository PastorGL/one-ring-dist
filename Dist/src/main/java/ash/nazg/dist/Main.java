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
import ash.nazg.storage.*;
import ash.nazg.storage.hadoop.FileStorage;
import com.google.common.collect.Lists;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Main {
    private static final Logger LOG = Logger.getLogger(Main.class);
    private static JavaSparkContext context;
    private static boolean local;
    private static StreamResolver dsResolver;
    private static boolean deleteOnSuccess = false;
    private static Map<String, Tuple3<String[], String[], Character>> sinkInfo;

    public static void main(String[] args) {
        TaskWrapperConfigBuilder configBuilder = new TaskWrapperConfigBuilder();

        try {
            configBuilder.addRequiredOption("c", "config", true, "Config file");
            configBuilder.addOption("d", "direction", true, "Copy direction. Can be 'from', 'to', or 'nop' to just validate the config file and exit");

            configBuilder.setCommandLine(args);

            SparkConf sparkConf = new SparkConf()
                    .setAppName("One Ring Dist")
                    .set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getCanonicalName());

            local = configBuilder.hasOption("local");
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

            TaskDefinitionLanguage.Task taskConfig = configBuilder.build(context);
            configBuilder.foreignLayerVariable("distcp.wrap", "d");
            configBuilder.foreignLayerVariable("distcp.store", "S");

            dsResolver = new StreamResolver(taskConfig.dataStreams);

            DistCpSettings settings = DistCpSettings.fromConfig(taskConfig.foreignLayer(Constants.DISTCP_LAYER));

            TaskDefinitionLanguage.Definitions props = taskConfig.foreignLayer(Constants.DISTCP_LAYER);
            LayerResolver distResolver = new LayerResolver(props);

            CpDirection distDirection = CpDirection.parse(distResolver.get("wrap", "nop"));
            if (distDirection == CpDirection.BOTH_DIRECTIONS) {
                throw new InvalidConfigValueException("One Ring Dist's copy direction can't be 'both' because it's ambiguous");
            }

            if (distDirection.anyDirection && settings.anyDirection) {
                String codec = distResolver.get("codec", "none");

                if (distDirection == CpDirection.FROM_CLUSTER) {
                    deleteOnSuccess = Boolean.parseBoolean(distResolver.get("move", "true"));
                }

                if (distDirection.toCluster && settings.toCluster) {
                    List<Tuple4<String, String, String, String>> inputs = new ArrayList<>();

                    sinkInfo = new HashMap<>();
                    for (String sink : taskConfig.sink) {
                        String path = dsResolver.inputPath(sink);

                        InputAdapter inputAdapter = Adapters.input(path).newInstance();
                        inputAdapter.initialize(context);
                        if (inputAdapter instanceof HadoopInput) {
                            sinkInfo.put(sink, new Tuple3<>(dsResolver.sinkSchema(sink), dsResolver.rawInputColumns(sink), dsResolver.inputDelimiter(sink)));

                            System.out.println("Sink: " + sink);
                            System.out.println("- schema: " + Arrays.toString(dsResolver.sinkSchema(sink)));
                            System.out.println("- columns: " + Arrays.toString(dsResolver.rawInputColumns(sink)));
                            System.out.println("- delimiter: " + dsResolver.inputDelimiter(sink));

                            List<Tuple3<String, String, String>> splits = FileStorage.srcDestGroup(path);
                            for (Tuple3<String, String, String> split : splits) {
                                inputs.add(new Tuple4<>(split._2(), settings.inputDir + "/" + sink, split._3(), sink));
                            }
                        }
                    }

                    distCpCmd(inputs, codec);
                }

                if (distDirection.fromCluster && settings.fromCluster) {
                    if (settings.wrapperStorePath != null) {
                        final String source = settings.wrapperStorePath + "/outputs/part-00000";
                        List<Tuple4<String, String, String, String>> outputs = context.wholeTextFiles(source.substring(0, source.lastIndexOf('/')))
                                .filter(t -> t._1.equals(source))
                                .flatMap(t -> {
                                    String[] s = t._2.split("\\R+");
                                    return Arrays.asList(s).iterator();
                                })
                                .collect().stream()
                                .map(output -> {
                                    String path = String.valueOf(output);
                                    String name = path.substring((settings.outputDir + "/").length());

                                    return new Tuple4<>(path, dsResolver.outputPath(name), ".*/(" + name + ".*?)/part.*", (String) null);
                                })
                                .collect(Collectors.toList());

                        distCpCmd(outputs, codec);
                    } else {
                        List<Tuple4<String, String, String, String>> teeList = new ArrayList<>();
                        for (String tee : taskConfig.tees) {
                            if (tee.endsWith("*")) {
                                throw new InvalidConfigValueException("A call of configuration with wildcard task.tee.output must" +
                                        " have wrapper store path set");
                            }

                            String path = dsResolver.outputPath(tee);
                            OutputAdapter outputAdapter = Adapters.output(path).newInstance();
                            outputAdapter.initialize(context);
                            if (outputAdapter instanceof HadoopOutput) {
                                if (StorageAdapter.PATH_PATTERN.matcher(path).matches()) {
                                    teeList.add(new Tuple4<>(settings.outputDir + "/" + tee, path, ".*/(" + tee + ".*?)/part.*", null));
                                } else {
                                    throw new InvalidConfigValueException("Output path '" + path + "' must point to a subdirectory for an output '" + tee + "'");
                                }
                            }
                        }

                        distCpCmd(teeList, codec);
                    }
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

    // from, to, group, ?sink
    private static void distCpCmd(List<Tuple4<String, String, String, String>> list, String codec) {
        JavaRDD<Tuple4<String, String, String, String>> srcDestGroups = context.parallelize(list);

        // sink?, dest -> files
        Map<Tuple2<String, String>, List<String>> discoveredFiles = srcDestGroups
                .mapToPair(srcDestGroup -> {
                    List<String> files = new ArrayList<>();
                    try {
                        Path srcPath = new Path(srcDestGroup._1());

                        Configuration conf = new Configuration();

                        FileSystem srcFS = srcPath.getFileSystem(conf);
                        RemoteIterator<LocatedFileStatus> srcFiles = srcFS.listFiles(srcPath, true);

                        Pattern pattern = Pattern.compile(srcDestGroup._3());

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

                    return new Tuple2<>(new Tuple2<>(srcDestGroup._4(), srcDestGroup._2()), files);
                })
                .combineByKey(t -> t, (c, t) -> {
                    c.addAll(t);
                    return c;
                }, (c1, c2) -> {
                    c1.addAll(c2);
                    return c1;
                })
                .collectAsMap();

        CopyFilesFunction cff = new CopyFilesFunction(deleteOnSuccess, codec, sinkInfo);

        List<Tuple3<List<String>, String, String>> regrouped = new ArrayList<>();

        int numOfExecutors = local ? 1 : (int) Math.ceil(Integer.parseInt(context.getConf().get("spark.executor.instances", "20")) * 0.8);
        numOfExecutors = Math.max(numOfExecutors, 1);
        for (Map.Entry<Tuple2<String, String>, List<String>> group : discoveredFiles.entrySet()) {
            int desiredNumber = numOfExecutors;

            String sink = group.getKey()._1;
            if (sink != null) {
                desiredNumber = dsResolver.inputParts(sink);
                if (desiredNumber <= 0) {
                    desiredNumber = numOfExecutors;
                }
            }

            List<String> sinkFiles = group.getValue();
            int countOfFiles = sinkFiles.size();

            int groupSize = countOfFiles / desiredNumber;
            if (groupSize <= 0) {
                groupSize = 1;
            }

            List<List<String>> sinkParts = Lists.partition(sinkFiles, groupSize);

            for (int i = 0; i < sinkParts.size(); i++) {
                List<String> sinkPart = sinkParts.get(i);

                regrouped.add(new Tuple3<>(new ArrayList<>(sinkPart), group.getKey()._2 + "/part-" + String.format("%05d", i), sink));
            }
        }

        context.parallelize(regrouped, regrouped.size())
                .foreach(cff);
    }
}
