package ash.nazg.storage.s3direct;

import alex.mojaki.s3upload.MultiPartOutputStream;
import alex.mojaki.s3upload.StreamTransferManager;
import ash.nazg.storage.hadoop.FileStorage;
import ash.nazg.storage.hadoop.PartOutputFunction;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.sparkproject.guava.collect.Iterators;

import java.io.OutputStream;
import java.util.Iterator;
import java.util.regex.Matcher;

public class S3DirectPartOutputFunction extends PartOutputFunction {
    private String accessKey;
    private String secretKey;

    private String contentType;
    private String endpoint;
    private String region;
    private String tmpDir;

    public S3DirectPartOutputFunction(String _name, String outputPath, String codec, String[] _columns, char _delimiter, String endpoint, String region, String accessKey, String secretKey, String tmpDir, String contentType) {
        super(_name, outputPath, codec, _columns, _delimiter);

        this.endpoint = endpoint;
        this.region = region;
        this.secretKey = secretKey;
        this.accessKey = accessKey;
        this.contentType = contentType;
        this.tmpDir = tmpDir;
    }

    @Override
    protected OutputStream createOutputStream(Configuration conf, int idx, Iterator<Object> it) throws Exception {
        String suffix = FileStorage.suffix(outputPath);

        Matcher m = S3DirectStorage.PATTERN.matcher(outputPath);
        m.matches();

        final String bucket = m.group(1);
        final String key = m.group(2);

        AmazonS3 _s3 = S3DirectStorage.get(endpoint, region, accessKey, secretKey);

            StreamTransferManager stm = new StreamTransferManager(bucket, key + "." + String.format("part-%05d", idx), _s3) {
                @Override
                public void customiseInitiateRequest(InitiateMultipartUploadRequest request) {
                    ObjectMetadata om = new ObjectMetadata();
                    om.setContentType(contentType);
                    request.setObjectMetadata(om);
                }
            };

            MultiPartOutputStream stream = stm.numStreams(1)
                    .numUploadThreads(1)
                    .queueCapacity(1)
                    .partSize(15)
                    .getMultiPartOutputStreams().get(0);


        if ("parquet".equalsIgnoreCase(suffix)) {
            Path partPath = new Path(outputPath, String.format("%05d", idx)
                    + (!"none".equalsIgnoreCase(codec) ? "." + codec : "")
                    + ".parquet");

            writeToParquetFile(conf, partPath, it);

            return null;
        } else {
            Path partPath = new Path(outputPath, String.format("%05d", idx)
                    + (!"none".equalsIgnoreCase(codec) ? "." + codec : ""));

            FileSystem outputFs = partPath.getFileSystem(conf);
            outputFs.setVerifyChecksum(false);
            OutputStream outputStream = outputFs.create(partPath);

            return writeToTextFile(conf, outputStream);
        }
    }
}
