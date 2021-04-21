package ash.nazg.storage.s3direct;

import ash.nazg.storage.hadoop.InputFunction;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class S3DirectInputFunction extends InputFunction {
    public S3DirectInputFunction(String[] schema, String[] columns, char delimiter, int bufferSize) {
        super(schema, columns, delimiter, bufferSize);
    }

    @Override
    public Iterator<Object> call(List<String> src) {
        it -> {
            AmazonS3ClientBuilder s3cb = AmazonS3ClientBuilder.standard()
                    .enableForceGlobalBucketAccess();
            if ((_accessKey != null) && (_secretKey != null)) {
                s3cb.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(_accessKey, _secretKey)));
            }
            AmazonS3 _s3 = s3cb.build();

            Stream<String> lines = null;
            while (it.hasNext()) {
                String key = it.next();

                Stream<String> file = new BufferedReader(new InputStreamReader(_s3.getObject(_bucket, key).getObjectContent(), StandardCharsets.UTF_8.name())).lines();
                if ((lines == null)) {
                    lines = file;
                } else {
                    lines = Stream.concat(lines, file);
                }
            }

            return lines.iterator();
        }
    }
}
