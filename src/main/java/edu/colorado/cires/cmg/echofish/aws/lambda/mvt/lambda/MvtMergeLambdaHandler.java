package edu.colorado.cires.cmg.echofish.aws.lambda.mvt.lambda;

import edu.colorado.cires.cmg.awszarr.S3ClientWrapper;
import edu.colorado.cires.cmg.echofish.aws.lambda.mvt.mvt.S3MvtStore;
import edu.colorado.cires.cmg.echofish.data.model.CruiseProcessingMessage;
import edu.colorado.cires.cmg.mvtset.MvtSetMerger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MvtMergeLambdaHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(MvtMergeLambdaHandler.class);

  private final S3ClientWrapper s3;
  private final MvtMergeLambdaConfiguration configuration;

  public MvtMergeLambdaHandler(S3ClientWrapper s3, MvtMergeLambdaConfiguration configuration) {
    this.s3 = s3;
    this.configuration = configuration;
  }

  public Void handleRequest(CruiseProcessingMessage snsMessage) {

    LOGGER.info("Started Event: {}", snsMessage);

    MvtMergeEventContext eventContext = new MvtMergeEventContext(
        configuration.getZarrBucketName(),
        snsMessage.getShipName(),
        snsMessage.getCruiseName(),
        snsMessage.getSensorName(),
        configuration.getMaxUploadBuffers()
    );

    LOGGER.info("Context: {}", eventContext);

    S3MvtStore sourceMvtStore = new S3MvtStore(
        s3,
        eventContext,
        (ec, index) -> "spatial/mvt/cruise/" + ec.getShipName() + "/" + ec.getCruiseName() + "/" + ec.getSensorName() + "/" + index + ".pbf",
        (ec) -> "spatial/mvt/cruise/" + ec.getShipName() + "/" + ec.getCruiseName() + "/" + ec.getSensorName() + "/");

    S3MvtStore targetMvtStore = new S3MvtStore(
        s3,
        eventContext,
        (ec, index) -> "spatial/mvt/global/" + index + ".pbf",
        (ec) -> "spatial/mvt/global/");

    MvtSetMerger mvtSetMerger = new MvtSetMerger(sourceMvtStore, targetMvtStore);
    mvtSetMerger.mergePyramid();

    LOGGER.info("Finished Event: {}", snsMessage);

    return null;
  }
}
