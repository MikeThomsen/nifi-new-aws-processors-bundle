package org.apache.nifi.processors.aws.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.AbstractAWSProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ClientType;

import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestExistsS3Object {
    private TestRunner runner = null;
    private ExistsS3Object mockExistsS3Object = null;
    private AmazonS3Client mockS3Client = null;

    @BeforeEach
    public void setUp() {
        mockS3Client = mock(AmazonS3Client.class);
        mockExistsS3Object = new ExistsS3Object() {
            @Override
            protected AbstractAWSProcessor<AmazonS3Client>.AWSConfiguration getConfiguration(ProcessContext context) {
                var config = mock(AbstractAWSProcessor.AWSConfiguration.class);

                when(config.getClient()).thenReturn(mockS3Client);

                return config;
            }
        };
        runner = TestRunners.newTestRunner(mockExistsS3Object);
//        AuthUtils.enableAccessKey(runner, "accessKeyId", "secretKey");
    }

    private void commonTest() {
        runner.setProperty(ExistsS3Object.BUCKET, "${s3.bucket}");
        runner.setProperty(ExistsS3Object.KEY, "${filename}");
        runner.enqueue("", Map.of("s3.bucket", "test-data", "filename", "test.txt"));

        runner.run();
    }

    @Test
    public void testRunExists() {
        when(mockS3Client.doesObjectExist(anyString(), anyString()))
                .thenReturn(true);
        commonTest();
        runner.assertTransferCount(ExistsS3Object.REL_FOUND, 1);
    }

    @Test
    public void testRunDoesNotExist() {
        when(mockS3Client.doesObjectExist(anyString(), anyString()))
                .thenReturn(false);
        commonTest();
        runner.assertTransferCount(ExistsS3Object.REL_NOT_FOUND, 1);
    }

    @Test
    public void testRunHasS3Error() {
        when(mockS3Client.doesObjectExist(anyString(), anyString()))
                .thenThrow(new RuntimeException("Manually triggered error"));
        commonTest();
        runner.assertTransferCount(ExistsS3Object.REL_FAILURE, 1);
    }
}
