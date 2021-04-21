package ash.nazg.storage.s3direct;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class S3DirectStorage {
    public static AmazonS3 get(String endpoint, String region, String accessKey, String secretKey) {
        AmazonS3ClientBuilder s3ClientBuilder = AmazonS3ClientBuilder.standard();
        if (endpoint != null) {
            s3ClientBuilder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region));
        }
        if ((accessKey != null) && (secretKey != null)) {
            s3ClientBuilder.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)));
        }

        AmazonS3 s3 = s3ClientBuilder
                .enableForceGlobalBucketAccess()
                .build();

    }
}
