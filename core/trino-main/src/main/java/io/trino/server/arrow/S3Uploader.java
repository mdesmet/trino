/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.trino.server.arrow;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.HttpMethod;
import com.amazonaws.Protocol;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class S3Uploader
{
    static final long PART_SIZE = 5 * 1024 * 1024; // 5MB
    private Regions region;
    private String bucketName;
    private AmazonS3 s3Client;

    public S3Uploader()
    {
        this(Regions.US_EAST_1, "hackathon-arrow");
    }

    public S3Uploader(Regions regions, String bucketName)
    {
        // TODO , inject credentials
        this.region = regions;
        this.bucketName = bucketName;

        ClientConfiguration config = new ClientConfiguration()
                .withMaxErrorRetry(3)
                .withProtocol(Protocol.HTTP);

        s3Client = AmazonS3ClientBuilder.standard()
                .withClientConfiguration(config)
                .withRegion(region)
                .build();
    }

    private static Date generateExpiration(int expirationPeriodInMs)
    {
        Date expiration = new Date();
        expiration.setTime(expiration.getTime() + expirationPeriodInMs);
        return expiration;
    }

    public static void main(String[] args)
            throws IOException
    {
        S3Uploader writer = new S3Uploader();

        int size = 100;
        VectorSchemRootGenerator gen = new VectorSchemRootGenerator();
        uploadArrowWithRows(writer, gen, size);

//        String str = VectorSchemRootGenerator.randomString(50_000_000);
//        URL url = writer.uploadFile("hello50M", str.getBytes(StandardCharsets.UTF_8));
//        System.out.println("Url: " + url);

//        URL url = writer.uploadFile("hello", "HelloWorld".getBytes(StandardCharsets.UTF_8));
//        System.out.println("Url: " + url);
    }

    private static void uploadArrowWithRows(S3Uploader writer, VectorSchemRootGenerator gen, int size)
            throws IOException
    {
        VectorSchemaRoot vectorSchemaRoot = gen.generate(size);
        URL url = writer.uploadFile(VectorSchemRootGenerator.randomName() + size, vectorSchemaRoot);
        System.out.println("Url: " + url);
    }

    public URL uploadFile(String keyName, VectorSchemaRoot vectorSchemaRoot)
            throws IOException
    {
        return uploadFile(keyName, ArrowWriter.toBytes(vectorSchemaRoot));
    }

    private URL uploadFile(String keyName, byte[] contentBytes)
            throws IOException
    {
//        createBucketIfNotExist(bucketName);

        try {
            InputStream inputStream = new ByteArrayInputStream(contentBytes);

            InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, keyName);
            InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
            String uploadId = initResponse.getUploadId();

            long contentLength = contentBytes.length;
            int partCount = (int) Math.ceil((double) contentLength / PART_SIZE);
            byte[] partBytes = new byte[(int) PART_SIZE];

            List<UploadPartResult> parts = new ArrayList<>();

            for (int i = 0; i < partCount; i++) {
                int bytesRead = inputStream.read(partBytes);
                if (bytesRead == -1) {
                    break;
                }

                UploadPartRequest uploadRequest = new UploadPartRequest()
                        .withBucketName(bucketName)
                        .withKey(keyName)
                        .withUploadId(uploadId)
                        .withPartNumber(i + 1)
                        .withInputStream(new ByteArrayInputStream(partBytes, 0, bytesRead))
                        .withPartSize(bytesRead);

                UploadPartResult partResult = s3Client.uploadPart(uploadRequest);
                parts.add(partResult);
            }

            List<PartETag> etags = parts.stream()
                    .map(UploadPartResult::getPartETag)
                    .toList();
            CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(bucketName, keyName, uploadId, etags);
            CompleteMultipartUploadResult completeResponse = s3Client.completeMultipartUpload(completeRequest);

            return genereatePresignedUrl(bucketName, keyName);
        }
        catch (IOException | AmazonClientException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void createBucketIfNotExist(String bucketName)
    {
        if (!s3Client.doesBucketExistV2(bucketName)) {
            s3Client.createBucket(bucketName);
        }
    }

    URL genereatePresignedUrl(String bucketName, String keyName)
    {
        Date expiration = generateExpiration(1000 * 60 * 60 * 12);

        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();
        try {
            GeneratePresignedUrlRequest generatePresignedUrlRequest = new GeneratePresignedUrlRequest(bucketName, keyName)
                    .withMethod(HttpMethod.GET)
                    .withExpiration(expiration);
            return s3.generatePresignedUrl(generatePresignedUrlRequest);
        }
        catch (AmazonServiceException e) {
            throw new RuntimeException("Cannot create presigned URL");
        }
    }
}
