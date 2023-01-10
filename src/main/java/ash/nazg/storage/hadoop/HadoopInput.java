/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.hadoop;

import ash.nazg.dist.InvalidConfigurationException;
import ash.nazg.metadata.AdapterMeta;
import ash.nazg.metadata.DataHolder;
import ash.nazg.metadata.DefinitionMetaBuilder;
import ash.nazg.storage.InputAdapter;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class HadoopInput extends InputAdapter {
    protected static final String SCHEMA_DEFAULT = "schema_default";
    protected static final String SCHEMA_FROM_FILE = "schema_from_file";
    protected static final String COLUMNS = "columns";
    protected static final String DELIMITER = "delimiter";
    protected static final String PART_COUNT = "part_count";
    protected static final String SUB_DIRS = "split_sub_dirs";

    protected boolean subs;

    protected int partCount;
    protected String[] schemaDefault;
    protected boolean schemaFromFile;
    protected String[] dsColumns;
    protected String dsDelimiter;

    protected int numOfExecutors;

    @Override
    protected AdapterMeta meta() {
        return new AdapterMeta("hadoop", "Default input adapter that utilizes available Hadoop FileSystems." +
                " Supports plain text, delimited text (CSV/TSV), and Parquet files, optionally compressed",

                new DefinitionMetaBuilder()
                        .def(SUB_DIRS, "If set, any first-level subdirectories under designated path will" +
                                        " be split to different streams", Boolean.class, false,
                                "By default, don't split")
                        .def(SCHEMA_DEFAULT, "Loose schema of input records (just column of field names," +
                                        " optionally with placeholders to skip some, denoted by underscores _)",
                                String[].class, null, "By default, don't set the schema." +
                                        " Depending of source file type, built-in schema may be used")
                        .def(SCHEMA_FROM_FILE, "Try to read schema from file (1st line of delimited text or Parquet metadata)",
                                Boolean.class, true, "By default, do try")
                        .def(COLUMNS, "Columns to select from the schema",
                                String[].class, null, "By default, don't select columns from the schema")
                        .def(DELIMITER, "Record column delimiter",
                                String.class, "\t", "By default, tabulation character")
                        .def(PART_COUNT, "Desired number of parts",
                                Integer.class, 1, "By default, one part")
                        .build()
        );
    }

    @Override
    protected void configure() throws InvalidConfigurationException {
        subs = resolver.get(SUB_DIRS);

        dsDelimiter = resolver.get(DELIMITER);

        schemaFromFile = resolver.get(SCHEMA_FROM_FILE);
        if (!schemaFromFile) {
            schemaDefault = resolver.get(SCHEMA_DEFAULT);
        }

        dsColumns = resolver.get(COLUMNS);

        partCount = Math.max(resolver.get(PART_COUNT), 1);

        int executors = Integer.parseInt(context.getConf().get("spark.executor.instances", "-1"));
        numOfExecutors = (executors <= 0) ? 1 : (int) Math.ceil(executors * 0.8);
        numOfExecutors = Math.max(numOfExecutors, 1);

        if (partCount <= 0) {
            partCount = numOfExecutors;
        }
    }

    @Override
    public List<DataHolder> load(String globPattern) {
        // path, regex
        List<Tuple2<String, String>> splits = HadoopStorage.srcDestGroup(globPattern);

        // files
        List<Tuple2<String, String>> discoveredFiles = context.parallelize(splits, numOfExecutors)
                .flatMap(srcDestGroup -> {
                    List<Tuple2<String, String>> files = new ArrayList<>();
                    try {
                        Path srcPath = new Path(srcDestGroup._1);

                        Configuration conf = new Configuration();

                        FileSystem srcFS = srcPath.getFileSystem(conf);
                        RemoteIterator<LocatedFileStatus> srcFiles = srcFS.listFiles(srcPath, true);

                        Pattern pattern = Pattern.compile(srcDestGroup._2);

                        while (srcFiles.hasNext()) {
                            String srcFile = srcFiles.next().getPath().toString();

                            Matcher m = pattern.matcher(srcFile);
                            if (m.matches()) {
                                files.add(new Tuple2<>(srcDestGroup._1, srcFile));
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Exception while enumerating files to copy: " + e.getMessage());
                        e.printStackTrace(System.err);
                        System.exit(13);
                    }

                    return files.iterator();
                })
                .collect();

        System.out.println("Discovered Hadoop FileSystem files:");
        discoveredFiles.stream().map(Tuple2::_2).forEach(System.out::println);

        Map<String, List<String>> prefixMap = new HashMap<>();

        if (subs) {
            for (Tuple2<String, String> file : discoveredFiles) {
                int prefixLen = file._1.length();
                if (file._1.charAt(prefixLen - 1) == '/') {
                    prefixLen--;
                }

                String ds = "";
                int p = file._2.substring(prefixLen).indexOf("/");
                if (p != -1) {
                    int l = file._2.substring(prefixLen).lastIndexOf("/");
                    if (l != p) {
                        ds = file._2.substring(p + 1, l);
                    }
                }
                prefixMap.compute(ds, (k, v) -> {
                    if (v == null) {
                        v = new ArrayList<>();
                    }
                    v.add(file._2);
                    return v;
                });
            }
        } else {
            prefixMap.put("", discoveredFiles.stream().map(Tuple2::_2).collect(Collectors.toList()));
        }

        List<DataHolder> ret = new ArrayList<>();
        for (Map.Entry<String, List<String>> ds : prefixMap.entrySet()) {
            List<String> files = ds.getValue();

            int groupSize = files.size() / partCount;
            if (groupSize <= 0) {
                groupSize = 1;
            }

            List<List<String>> partNum = new ArrayList<>();
            Lists.partition(files, groupSize).forEach(p -> partNum.add(new ArrayList<>(p)));

            InputFunction inputFunction = new InputFunction(schemaDefault, dsColumns, dsDelimiter.charAt(0));
            return Collections.singletonList(new DataHolder(context.parallelize(partNum, partNum.size())
                    .flatMap(inputFunction.build()), ds.getKey()));
        }

        return ret;
    }
}
