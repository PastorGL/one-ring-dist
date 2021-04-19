/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.s3direct;

import alex.mojaki.s3upload.MultiPartOutputStream;
import alex.mojaki.s3upload.StreamTransferManager;
import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.storage.OutputAdapter;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.sparkproject.guava.collect.Iterators;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings("unused")
public class S3DirectOutput extends OutputAdapter {
    private static final Pattern PATTERN = Pattern.compile("^s3d://([^/]+)/(.+)");

    private int partCount;

    private String accessKey;
    private String secretKey;

    private String contentType;
    private char delimiter;

    @Description("S3 Direct adapter for any S3-compatible storage")
    public Pattern proto() {
        return PATTERN;
    }

    @Override
    protected void configure() throws InvalidConfigValueException {
        accessKey = outputResolver.get("access.key." + name);
        secretKey = outputResolver.get("secret.key." + name);

        contentType = outputResolver.get("content.type." + name, "text/csv");

        delimiter = dsResolver.outputDelimiter(name);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void save(String path, JavaRDD rdd) {
        Matcher m = PATTERN.matcher(path);
        m.matches();

        final String bucket = m.group(1);
        final String key = m.group(2);

        final String _accessKey = accessKey;
        final String _secretKey = secretKey;
        final String _contentType = contentType;

        ((JavaRDD<Object>) rdd).mapPartitionsWithIndex((partNumber, partition) -> {
            AmazonS3ClientBuilder s3ClientBuilder = AmazonS3ClientBuilder.standard()
                    .enableForceGlobalBucketAccess();
            if ((_accessKey != null) && (_secretKey != null)) {
                s3ClientBuilder.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(_accessKey, _secretKey)));
            }

            AmazonS3 _client = s3ClientBuilder.build();

            StreamTransferManager stm = new StreamTransferManager(bucket, path + "." + partNumber, _client) {
                @Override
                public void customiseInitiateRequest(InitiateMultipartUploadRequest request) {
                    ObjectMetadata om = new ObjectMetadata();
                    om.setContentType(_contentType);
                    request.setObjectMetadata(om);
                }
            };

            MultiPartOutputStream stream = stm.numStreams(1)
                    .numUploadThreads(1)
                    .queueCapacity(1)
                    .partSize(15)
                    .getMultiPartOutputStreams().get(0);
            while (partition.hasNext()) {
                Object v = partition.next();

                byte[] buf = null;
                int len = 0;
                if (v instanceof String) {
                    String s = (String) v;
                    buf = s.getBytes();
                    len = buf.length;
                }
                if (v instanceof Text) {
                    Text t = (Text) v;
                    buf = t.getBytes();
                    len = t.getLength();
                }

                stream.write(buf, 0, len);
            }
            stream.close();
            stm.complete();

            return Iterators.emptyIterator();
        }, true).count();
    }
}
