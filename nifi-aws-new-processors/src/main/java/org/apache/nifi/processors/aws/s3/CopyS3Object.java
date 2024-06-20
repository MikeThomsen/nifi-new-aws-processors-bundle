package org.apache.nifi.processors.aws.s3;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.List;


public class CopyS3Object extends AbstractS3Processor {
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
        final String targetBucket = context.getProperty(TARGET_BUCKET).evaluateAttributeExpressions(flowFile).getValue();
        final String targetKey = context.getProperty(TARGET_KEY).evaluateAttributeExpressions(flowFile).getValue();

        try {
            CopyObjectRequest request = new CopyObjectRequest(sourceBucket, sourceKey, targetBucket, targetKey);
            AccessControlList acl = createACL(context, flowFile);
            if (acl != null) {
                request.setAccessControlList(acl);
            }
            CannedAccessControlList cannedAccessControlList = createCannedACL(context, flowFile);

            if (cannedAccessControlList != null) {
                request.setCannedAccessControlList(cannedAccessControlList);
            }

            s3.copyObject(request);
            session.getProvenanceReporter().send(flowFile, getTransitUrl(targetBucket, targetKey));

            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception ex) {
            getLogger().error("Copy S3 Object Request failed with error:", ex);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private String getTransitUrl(String targetBucket, String targetKey) {
        String spacer = targetKey.startsWith("/") ? "" : "/";
        return String.format("s3://%s%s%s", targetBucket, spacer, targetKey);
    }
}
