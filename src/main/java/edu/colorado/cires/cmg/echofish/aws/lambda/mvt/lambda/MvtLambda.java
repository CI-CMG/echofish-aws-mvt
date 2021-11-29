package edu.colorado.cires.cmg.echofish.aws.lambda.mvt.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import edu.colorado.cires.cmg.awszarr.AwsS3ClientWrapper;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

public class MvtLambda implements RequestHandler<SNSEvent, Void> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MvtLambda.class);

  private static final MvtLambdaHandler HANDLER = new MvtLambdaHandler(
      AwsS3ClientWrapper.builder().s3(S3Client.builder().build()).build(),
      new MvtLambdaConfiguration(
          Objects.requireNonNull(System.getenv("ZARR_BUCKET_NAME")),
          Long.parseLong(System.getenv("TRACKLINE_SPLIT_MS")),
          Integer.parseInt(System.getenv("BATCH_SIZE")),
          Integer.parseInt(System.getenv("GEO_JSON_PRECISION")),
          Integer.parseInt(System.getenv("MAX_ZOOM_LEVEL")),
          Double.parseDouble(System.getenv("MIN_SIMPLIFICATION_TOLERANCE")),
          Double.parseDouble(System.getenv("MAX_SIMPLIFICATION_TOLERANCE")),
          Objects.requireNonNull(System.getenv("MVT_SURVEY_BUCKET_NAME")),
          Integer.parseInt(System.getenv("S3_UPLOAD_BUFFERS"))
      ));

  @Override
  public Void handleRequest(SNSEvent snsEvent, Context context) {

    LOGGER.info("Received event: {}", snsEvent);

    SnsMessage snsMessage;
    try {
      snsMessage = TheObjectMapper.OBJECT_MAPPER.readValue(snsEvent.getRecords().get(0).getSNS().getMessage(), SnsMessage.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Unable to parse SNS notification", e);
    }

    HANDLER.handleRequest(snsMessage);

    return null;
  }
}
