package edu.colorado.cires.cmg.echofish.aws.lambda.mvt.lambda;

import static org.junit.jupiter.api.Assertions.*;

import edu.colorado.cires.cmg.awszarr.FileMockS3ClientWrapper;
import edu.colorado.cires.cmg.awszarr.S3ClientWrapper;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class MvtLambdaHandlerTest {

  private final Path MOCK_BUCKET_DIR = Paths.get("target/mock-buckets");
  private final String BUCKET_NAME = "my-zarr-bucket";

//  @Test
//  public void test() {
//    final String zarrBucketName = BUCKET_NAME;
//    final long msSplit = 1000 * 60 * 60;
//    final int batchSize = 10000;
//    final int geoJsonPrecision = 5;
//    final int maxZoom = 5;
//    final double minSimplification = 0.00001;
//    final double maxSimplification = 0.1;
//    final String survey = "MY_SURVEY";
//    final String mvtSurveyBucketName = "my-survey-mvt-bucket";
//    final int maxUploadBuffers = 3;
//
//    final S3ClientWrapper s3 = FileMockS3ClientWrapper.builder().mockBucketDir(MOCK_BUCKET_DIR).build();
//    final MvtLambdaConfiguration configuration = new MvtLambdaConfiguration(
//        zarrBucketName,
//        msSplit,
//        batchSize,
//        geoJsonPrecision,
//        maxZoom,
//        minSimplification,
//        maxSimplification,
//        mvtSurveyBucketName,
//        maxUploadBuffers
//    );
//
//    final MvtLambdaHandler handler = new MvtLambdaHandler(s3, configuration);
//
//    final SnsMessage message = new SnsMessage();
//    message.setSurvey(survey);
//
//    handler.handleRequest(message);
//
//  }
}