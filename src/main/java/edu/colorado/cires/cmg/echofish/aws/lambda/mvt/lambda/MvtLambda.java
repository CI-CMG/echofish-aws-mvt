package edu.colorado.cires.cmg.echofish.aws.lambda.mvt.lambda;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.colorado.cires.cmg.awszarr.AwsS3ClientWrapper;
import edu.colorado.cires.cmg.echofish.data.model.CruiseProcessingMessage;
import edu.colorado.cires.cmg.echofish.data.model.jackson.ObjectMapperCreator;
import edu.colorado.cires.cmg.echofish.data.s3.S3OperationsImpl;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class MvtLambda implements RequestHandler<SNSEvent, Void> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MvtLambda.class);
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperCreator.create();

  private static final AwsCredentialsProvider creds = StaticCredentialsProvider.create(AwsBasicCredentials.create(
      Objects.requireNonNull(System.getenv("ZARR_BUCKET_ACCESS_KEY")),
      Objects.requireNonNull(System.getenv("ZARR_BUCKET_SECRET_ACCESS_KEY"))
  ));
  private static final S3Client s3 = S3Client.builder()
      .credentialsProvider(creds)
      .region(Region.of(System.getenv("BUCKET_REGION")))
      .build();

  private static final MvtLambdaHandler HANDLER = new MvtLambdaHandler(
      AwsS3ClientWrapper.builder().s3(s3).build(),
      new MvtLambdaConfiguration(
          Objects.requireNonNull(System.getenv("ZARR_BUCKET_NAME")),
          Long.parseLong(System.getenv("TRACKLINE_SPLIT_MS")),
          Integer.parseInt(System.getenv("BATCH_SIZE")),
          Integer.parseInt(System.getenv("GEO_JSON_PRECISION")),
          Integer.parseInt(System.getenv("MAX_ZOOM_LEVEL")),
          Double.parseDouble(System.getenv("MIN_SIMPLIFICATION_TOLERANCE")),
          Double.parseDouble(System.getenv("MAX_SIMPLIFICATION_TOLERANCE")),
          Integer.parseInt(System.getenv("S3_UPLOAD_BUFFERS"))
      ),
      new S3OperationsImpl(AmazonS3ClientBuilder.standard()
          .withCredentials(new AWSStaticCredentialsProvider(new BasicSessionCredentials(
              Objects.requireNonNull(System.getenv("ZARR_BUCKET_ACCESS_KEY")),
              Objects.requireNonNull(System.getenv("ZARR_BUCKET_SECRET_ACCESS_KEY")),
              null)))
          .withRegion(Objects.requireNonNull(System.getenv("BUCKET_REGION")))
          .build()));

  @Override
  public Void handleRequest(SNSEvent snsEvent, Context context) {


    LOGGER.info("Received event: {}", snsEvent);

    CruiseProcessingMessage cruiseProcessingMessage;
    try {
      cruiseProcessingMessage = OBJECT_MAPPER.readValue(snsEvent.getRecords().get(0).getSNS().getMessage(), CruiseProcessingMessage.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Unable to parse SNS notification", e);
    }

    HANDLER.handleRequest(cruiseProcessingMessage);

    return null;
  }
}
