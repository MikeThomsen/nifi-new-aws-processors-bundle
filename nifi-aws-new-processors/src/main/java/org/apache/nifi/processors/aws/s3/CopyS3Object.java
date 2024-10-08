package org.apache.nifi.processors.aws.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;


public class CopyS3Object extends AbstractS3Processor {
    public static final long MULTIPART_THRESHOLD = 5L * 1024L * 1024L * 1024L;

    public static final PropertyDescriptor SOURCE_BUCKET = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(BUCKET)
            .name("copy-s3-object-source-bucket")
            .displayName("Source Bucket")
            .description("The bucket that contains the file to be copied.")
            .build();
    public static final PropertyDescriptor TARGET_BUCKET = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(BUCKET)
            .name("copy-s3-object-target-bucket")
            .displayName("Target Bucket")
            .description("The bucket that will receive the copy.")
            .build();

    public static final PropertyDescriptor SOURCE_KEY = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(KEY)
            .name("copy-s3-source-key")
            .displayName("Source Key")
            .description("The source key in the source bucket")
            .build();

    public static final PropertyDescriptor TARGET_KEY = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(KEY)
            .name("copy-s3-target-key")
            .displayName("Target Key")
            .description("The target key in the target bucket")
            .defaultValue("")
            .build();

    public static final List<PropertyDescriptor> properties = List.of(
            SOURCE_BUCKET,
            SOURCE_KEY,
            TARGET_BUCKET,
            TARGET_KEY,
            AWS_CREDENTIALS_PROVIDER_SERVICE,
            REGION,
            TIMEOUT,
            FULL_CONTROL_USER_LIST,
            READ_USER_LIST,
            WRITE_USER_LIST,
            READ_ACL_LIST,
            WRITE_ACL_LIST,
            CANNED_ACL,
            OWNER,
            SSL_CONTEXT_SERVICE,
            ENDPOINT_OVERRIDE,
            SIGNER_OVERRIDE,
            S3_CUSTOM_SIGNER_CLASS_NAME,
            S3_CUSTOM_SIGNER_MODULE_LOCATION,
            PROXY_CONFIGURATION_SERVICE);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final AmazonS3Client s3;
        try {
            s3 = getConfiguration(context).getClient();
        } catch (Exception e) {
            getLogger().error("Failed to initialize S3 client", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final String sourceBucket = context.getProperty(SOURCE_BUCKET).evaluateAttributeExpressions(flowFile).getValue();
        final String sourceKey = context.getProperty(SOURCE_KEY).evaluateAttributeExpressions(flowFile).getValue();
        final String destinationBucket = context.getProperty(TARGET_BUCKET).evaluateAttributeExpressions(flowFile).getValue();
        final String destinationKey = context.getProperty(TARGET_KEY).evaluateAttributeExpressions(flowFile).getValue();

        final AtomicReference<String> multipartIdRef = new AtomicReference<>();
        final boolean isMultiPart = flowFile.getSize() > MULTIPART_THRESHOLD;

        try {
            if (!isMultiPart) {
                final CopyObjectRequest request = new CopyObjectRequest(sourceBucket, sourceKey, destinationBucket, destinationKey);
                final AccessControlList acl = createACL(context, flowFile);
                if (acl != null) {
                    request.setAccessControlList(acl);
                }

                final CannedAccessControlList cannedAccessControlList = createCannedACL(context, flowFile);
                if (cannedAccessControlList != null) {
                    request.setCannedAccessControlList(cannedAccessControlList);
                }

                s3.copyObject(request);
            } else {
                GetObjectMetadataRequest sourceMetadataRequest = new GetObjectMetadataRequest(sourceBucket, sourceKey);
                ObjectMetadata metadataResult = s3.getObjectMetadata(sourceMetadataRequest);

                if (metadataResult == null) {
                    throw new ProcessException("The ObjectMetadata was null");
                }

                InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(destinationBucket,
                        destinationKey);
                InitiateMultipartUploadResult initResult = s3.initiateMultipartUpload(initRequest);

                multipartIdRef.set(initResult.getUploadId());

                long objectSize = metadataResult.getContentLength();

                long bytePosition = 0;
                int partNumber = 1;
                List<CopyPartResult> responses = new ArrayList<>();
                while (bytePosition < objectSize) {
                    long lastByte = Math.min(bytePosition + MULTIPART_THRESHOLD - 1, objectSize - 1);

                    CopyPartRequest copyRequest = new CopyPartRequest()
                            .withSourceBucketName(sourceBucket)
                            .withSourceKey(sourceKey)
                            .withDestinationBucketName(destinationBucket)
                            .withDestinationKey(destinationKey)
                            .withUploadId(initResult.getUploadId())
                            .withFirstByte(bytePosition)
                            .withLastByte(lastByte)
                            .withPartNumber(partNumber++);
                    boolean partIsDone = false;
                    int retryIndex = 0;

                    while (!partIsDone && retryIndex < 3) {
                        try {
                            responses.add(s3.copyPart(copyRequest));
                            partIsDone = true;
                        } catch (AmazonS3Exception e) {
                            if (e.getStatusCode() == 503) {
                                retryIndex++;
                            } else {
                                throw e;
                            }
                        }
                    }
                    bytePosition += MULTIPART_THRESHOLD;
                }

                CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(
                        destinationBucket,
                        destinationKey,
                        initResult.getUploadId(),
                        responses.stream().map(response -> new PartETag(response.getPartNumber(), response.getETag()))
                                .collect(Collectors.toList()));
                s3.completeMultipartUpload(completeRequest);
            }
            session.getProvenanceReporter().send(flowFile, getTransitUrl(destinationBucket, destinationKey));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final ProcessException | IllegalArgumentException | AmazonClientException e) {
            if (isMultiPart) {
                String requestId = multipartIdRef.get();
                if (StringUtils.isNotBlank(requestId)) {
                    try {
                        AbortMultipartUploadRequest abortRequest = new AbortMultipartUploadRequest(destinationBucket, destinationKey, requestId);
                        s3.abortMultipartUpload(abortRequest);
                    } catch (AmazonS3Exception ignored) {
                        getLogger().error("Failed to cleanup the partial upload to bucket {} and key {}", destinationBucket, destinationKey);
                        getLogger().error("Abort exception", ignored);
                    }
                }
            }

            flowFile = extractExceptionDetails(e, session, flowFile);
            getLogger().error("Failed to copy S3 object from Bucket [{}] Key [{}]", sourceBucket, sourceKey, e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private String getTransitUrl(String targetBucket, String targetKey) {
        String spacer = targetKey.startsWith("/") ? "" : "/";
        return String.format("s3://%s%s%s", targetBucket, spacer, targetKey);
    }
}
