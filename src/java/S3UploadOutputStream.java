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

import java.io.OutputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;

public class S3UploadOutputStream extends BaseOutputStream {

    AmazonS3 s3Client;
    ArrayList<PartETag> partETags;
    InitiateMultipartUploadRequest initRequest;
    InitiateMultipartUploadResult initResult;
    String bucket;
    String key;
    int partNumber;
    boolean closed;

    public static OutputStream create(AmazonS3 client, String bucket, String key) {
        return new BufferedOutputStream(
                new S3UploadOutputStream(client, bucket, key),
                5 * 1000 * 1000);
    }

    S3UploadOutputStream(AmazonS3 client, String bucket, String key) {
        this.s3Client = client;
        this.bucket = bucket;
        this.key = key;
        this.partETags = new ArrayList<PartETag>();
        this.initRequest = new InitiateMultipartUploadRequest(bucket, key);
        this.initResult = client.initiateMultipartUpload(initRequest);
        this.partNumber = 1;
        this.closed = false;
    }

    public void flush() {
        // no buffereing here so noop
    }

    public synchronized void write(byte[] arr) {
        UploadPartRequest req = new UploadPartRequest()
            .withBucketName(this.bucket)
            .withKey(this.key)
            .withUploadId(this.initResult.getUploadId())
            .withPartNumber(this.partNumber)
            .withInputStream(new ByteArrayInputStream(arr))
            .withPartSize(arr.length);
        this.partETags.add(this.s3Client.uploadPart(req).getPartETag());
        this.partNumber++;
    }

    public synchronized void close() {
        if (this.closed) return;
        CompleteMultipartUploadRequest compRequest = new 
            CompleteMultipartUploadRequest(
                    this.bucket,
                    this.key,
                    this.initResult.getUploadId(), 
                    this.partETags);
        this.s3Client.completeMultipartUpload(compRequest);
        this.closed = true;
    }

    protected void finalize() {
        if (!this.closed) {
            s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(
                this.bucket, this.key, this.initResult.getUploadId()));
        }
    }

}
