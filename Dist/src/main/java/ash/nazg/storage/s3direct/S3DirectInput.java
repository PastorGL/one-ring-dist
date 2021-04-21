/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.s3direct;

import ash.nazg.config.tdl.Description;
import ash.nazg.storage.hadoop.HadoopInput;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public class S3DirectInput extends HadoopInput {
    private static final Pattern PATTERN = Pattern.compile("^s3d://([^/]+)/(.+)");

    private int partCount;

    private String accessKey;
    private String secretKey;
    private String region;
    private String endpoint;

    @Description("S3 Direct adapter for any S3-compatible storage")
    public Pattern proto() {
        return PATTERN;
    }

    @Override
    protected void configure() {
        super.configure();

        accessKey = inputResolver.get("s3d.access.key." + name);
        secretKey = inputResolver.get("s3d.secret.key." + name);
        region = inputResolver.get("s3d.region." + name);
        endpoint = inputResolver.get("s3d.endpoint." + name);

        partCount = Math.max(dsResolver.inputParts(name), 1);
    }

    @Override
    public JavaRDD load(String path) {
        Matcher m = PATTERN.matcher(path);
        m.matches();
        String bucket = m.group(1);
        String keyPrefix = m.group(2);

        AmazonS3 s3 = S3DirectStorage.get(endpoint, region, accessKey, secretKey);

        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(bucket);
        request.setPrefix(keyPrefix);

        List<String> s3FileKeys = s3.listObjects(request).getObjectSummaries().stream()
                .map(S3ObjectSummary::getKey)
                .collect(Collectors.toList());

        final String _accessKey = accessKey;
        final String _secretKey = secretKey;
        final String _bucket = bucket;

        FlatMapFunction<List<String>, Object> inputFunction = new S3DirectInputFunction(sinkSchema, sinkColumns, sinkDelimiter, maxRecordSize);

        return context.parallelize(s3FileKeys, partCount)
                .flatMap(inputFunction);
    }
}
