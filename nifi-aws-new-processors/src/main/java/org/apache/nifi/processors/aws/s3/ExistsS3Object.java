package org.apache.nifi.processors.aws.s3;

import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ExistsS3Object extends AbstractS3Processor {
    public static final List<PropertyDescriptor> properties = List.of(
            BUCKET,
            KEY,
            AWS_CREDENTIALS_PROVIDER_SERVICE,
            REGION,
            TIMEOUT,
            FULL_CONTROL_USER_LIST,
            READ_USER_LIST,
            WRITE_USER_LIST,
            READ_ACL_LIST,
            WRITE_ACL_LIST,
            OWNER,
            SSL_CONTEXT_SERVICE,
            ENDPOINT_OVERRIDE,
            SIGNER_OVERRIDE,
            S3_CUSTOM_SIGNER_CLASS_NAME,
            S3_CUSTOM_SIGNER_MODULE_LOCATION,
            PROXY_CONFIGURATION_SERVICE);

    public static Relationship REL_FOUND = new Relationship.Builder()
            .name("found")
            .description("An object was found in the bucket at the supplied key")
            .build();

    public static Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not found")
            .description("No object was found in the bucket the supplied key")
            .build();

    @Override
    public Set<Relationship> getRelationships() {
        return new HashSet<>(List.of(REL_FOUND, REL_NOT_FOUND, REL_FAILURE));
    }

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

        final String bucket = context.getProperty(BUCKET).evaluateAttributeExpressions(flowFile).getValue();
        final String key = context.getProperty(KEY).evaluateAttributeExpressions(flowFile).getValue();

        try {
            boolean exists = s3.doesObjectExist(bucket, key);

            session.transfer(flowFile, exists ? REL_FOUND : REL_NOT_FOUND);
        } catch (Exception ex) {
            getLogger().error("There was a problem checking for " + String.format("s3://%s%s", bucket, key), ex);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
