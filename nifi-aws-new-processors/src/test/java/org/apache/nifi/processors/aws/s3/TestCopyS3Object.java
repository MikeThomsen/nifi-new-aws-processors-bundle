package org.apache.nifi.processors.aws.s3;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.AbstractAWSProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestCopyS3Object {
    private TestRunner runner = null;
    private CopyS3Object mockCopyS3Object = null;
    private AmazonS3Client mockS3Client = null;

    @BeforeEach
    public void setUp() {
        mockS3Client = mock(AmazonS3Client.class);

        mockCopyS3Object = new CopyS3Object() {
            @Override
            protected AbstractAWSProcessor<AmazonS3Client>.AWSConfiguration getConfiguration(ProcessContext context) {
                var config = mock(AbstractAWSProcessor.AWSConfiguration.class);

                when(config.getClient()).thenReturn(mockS3Client);

                return config;
            }
        };
        runner = TestRunners.newTestRunner(mockCopyS3Object);
//        AuthUtils.enableAccessKey(runner, "accessKeyId", "secretKey");
    }

    @Test
    public void testRun() {
        runner.enqueue("".getBytes(StandardCharsets.UTF_8), setupRun());
        runner.run();

        runner.assertTransferCount(CopyS3Object.REL_SUCCESS, 1);

        verify(mockS3Client, times(1))
                .copyObject(any(CopyObjectRequest.class));

        var provenanceEvents = runner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
    }

    @Test
    public void testS3ErrorHandling() {
        when(mockS3Client.copyObject(any(CopyObjectRequest.class)))
                .thenThrow(new RuntimeException("Manually triggered error"));

        runner.enqueue("".getBytes(StandardCharsets.UTF_8), setupRun());
        runner.run();

        runner.assertTransferCount(CopyS3Object.REL_FAILURE, 1);
    }

    private Map<String, String> setupRun() {
        runner.setProperty(CopyS3Object.SOURCE_BUCKET, "${s3.bucket.source}");
        runner.setProperty(CopyS3Object.SOURCE_KEY, "${s3.key.source}");
        runner.setProperty(CopyS3Object.TARGET_BUCKET, "${s3.bucket.target}");
        runner.setProperty(CopyS3Object.TARGET_KEY, "${s3.key.target}");

        return Map.of("s3.bucket.source", "dev-bucket",
                "s3.key.source", "/test.txt",
                "s3.bucket.target", "staging-bucket",
                "s3.key.target", "/fakeproddata.txt");
    }
}
