package edu.colorado.cires.cmg.echofish.aws.lambda.mvt.lambda;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import edu.colorado.cires.cmg.awszarr.FileMockS3ClientWrapper;
import edu.colorado.cires.cmg.echofish.aws.test.MockS3Operations;
import edu.colorado.cires.cmg.echofish.aws.test.S3TestUtils;
import edu.colorado.cires.cmg.echofish.data.model.CruiseProcessingMessage;
import edu.colorado.cires.cmg.echofish.data.s3.S3Operations;
import edu.colorado.cires.cmg.echofish.data.sns.SnsNotifier;
import edu.colorado.cires.cmg.echofish.data.sns.SnsNotifierFactory;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HandlerTest {

    private static final Path BUCKET_BASE = Paths.get("target/output").toAbsolutePath();
    private static final String ZARR_BUCKET = "zarr-bucket";
    private static final String TOPIC_ARN = "my-topic";

    private MvtLambdaHandler handler;
    private SnsNotifierFactory sns;

    private static class CustomMockS3Operations implements S3Operations {

        private final MockS3Operations s3Operations = new MockS3Operations();

        @Override
        public void copyObject(String sourceBucket, String sourceKey, String targetBucket, String targetKey) {
            s3Operations.copyObject(BUCKET_BASE.resolve(sourceBucket).toString(), sourceKey, BUCKET_BASE.resolve(targetBucket).toString(), targetKey);
        }

        @Override
        public List<String> deleteObjects(String bucket, List<String> keys) {
            return s3Operations.deleteObjects(BUCKET_BASE.resolve(bucket).toString(), keys);
        }

        @Override
        public boolean doesObjectExist(String bucket, String key) {
            return s3Operations.doesObjectExist(BUCKET_BASE.resolve(bucket).toString(), key);
        }

        @Override
        public List<String> listObjects(String bucket, String prefix) {
            return s3Operations.listObjects(BUCKET_BASE.resolve(bucket).toString(), prefix);
        }

        @Override
        public List<String> listObjects(String bucket) {
            return s3Operations.listObjects(BUCKET_BASE.resolve(bucket).toString());
        }

        @Override
        public void uploadDirectoryToBucket(Path dir, String bucket) {
            s3Operations.uploadDirectoryToBucket(dir, BUCKET_BASE.resolve(bucket).toString());
        }

        @Override
        public void upload(Path source, String targetBucket, String targetKey) {
            s3Operations.upload(source, BUCKET_BASE.resolve(targetBucket).toString(), targetKey);
        }

        @Override
        public void download(String sourceBucket, String sourceKey, Path target) {
            s3Operations.download(BUCKET_BASE.resolve(sourceBucket).toString(), sourceKey, target);
        }
    }

    private static final long splitMs = 900000L;
    private static final int batchSize = 10000;
    private final int geoJsonPrecision = 5;
    private final int maxZoom = 5;
    private final double minSimplification = 0.00001;
    private final double maxSimplification = 0.1;
    private final int maxUploadBuffers = 3;


    @BeforeEach
    public void before() throws Exception {
        S3TestUtils.cleanupMockS3Directory(BUCKET_BASE);
        Files.createDirectories(BUCKET_BASE.resolve(ZARR_BUCKET));
        FileUtils.copyDirectory(
            new File("src/test/resources/echofish-dev-output/level_2/Henry_B._Bigelow/HB0707/EK60/HB0707_2.zarr"),
            BUCKET_BASE.resolve(ZARR_BUCKET).resolve("level_2/Henry_B._Bigelow/HB0707/EK60/HB0707.zarr").toFile()
            );
        sns = mock(SnsNotifierFactory.class);

        handler = new MvtLambdaHandler(
            FileMockS3ClientWrapper.builder().mockBucketDir(BUCKET_BASE).build(),
            new MvtLambdaConfiguration(
                ZARR_BUCKET,
                splitMs,
                batchSize,
                geoJsonPrecision,
                maxZoom,
                minSimplification,
                maxSimplification,
                maxUploadBuffers,
                TOPIC_ARN),
            new CustomMockS3Operations(), sns);

    }

    @AfterEach
    public void after() throws Exception {
        S3TestUtils.cleanupMockS3Directory(BUCKET_BASE);
    }


    @Test
    public void testCreateMvt() throws Exception {

        SnsNotifier snsNotifier = mock(SnsNotifier.class);
        when(sns.createNotifier()).thenReturn(snsNotifier);

        CruiseProcessingMessage message = new CruiseProcessingMessage();
        message.setCruiseName("HB0707");
        message.setShipName("Henry_B._Bigelow");
        message.setSensorName("EK60");

        handler.handleRequest(message);

        assertTrue(Files.exists(BUCKET_BASE.resolve(ZARR_BUCKET).resolve("spatial/mvt/cruise/Henry_B._Bigelow/HB0707/EK60/0/0/0.pbf")));

        verify(snsNotifier).notify(eq(TOPIC_ARN), eq(message));
    }

}