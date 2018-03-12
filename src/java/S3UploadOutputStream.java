package co.momomo;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.OutputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;

public class S3UploadOutputStream extends BaseOutputStream {

    AmazonS3 s3Client;
    String bucket;
    String key;
    int partNumber;
    boolean closed;

    public static OutputStream create(AmazonS3 client, String bucket, String key) {
        return new BufferedOutputStream(
                new S3UploadOutputStream(client, bucket, key),
                20 * 1000 * 1000);
    }

    S3UploadOutputStream(AmazonS3 client, String bucket, String key) {
        this.s3Client = client;
        this.bucket = bucket;
        this.key = key;
        this.partNumber = 1;
        this.closed = false;
    }

    public void flush() {
        // no buffereing here so noop
    }

    public synchronized void write(byte[] arr) {
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(arr.length);
        this.s3Client.putObject(new PutObjectRequest(
                    this.bucket, String.format("%s-%d", this.key, this.partNumber),
                    new ByteArrayInputStream(arr),
                    meta));
        this.partNumber++;
    }

    public synchronized void close() {
        if (this.closed) return;
        this.closed = true;
    }

}
