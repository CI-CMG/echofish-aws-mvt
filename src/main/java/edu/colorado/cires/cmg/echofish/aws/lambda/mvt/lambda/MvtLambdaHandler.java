package edu.colorado.cires.cmg.echofish.aws.lambda.mvt.lambda;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.colorado.cires.cmg.awszarr.S3ClientWrapper;
import edu.colorado.cires.cmg.echofish.aws.lambda.mvt.mvt.AwsS3MvtStore;
import edu.colorado.cires.cmg.echofish.aws.lambda.mvt.mvt.GeoJsonToMvtPipe;
import edu.colorado.cires.cmg.echofish.aws.lambda.mvt.zarr.ZarrToGeoJsonPipe;
import edu.colorado.cires.cmg.echofish.data.model.CruiseProcessingMessage;
import edu.colorado.cires.cmg.echofish.data.model.jackson.ObjectMapperCreator;
import edu.colorado.cires.cmg.echofish.data.s3.S3Operations;
import edu.colorado.cires.cmg.mvtset.MvtStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MvtLambdaHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(MvtLambdaHandler.class);
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperCreator.create();

  private final S3ClientWrapper s3;
  private final MvtLambdaConfiguration configuration;
  private final S3Operations s3Ops;

  public MvtLambdaHandler(S3ClientWrapper s3, MvtLambdaConfiguration configuration, S3Operations s3Ops) {
    this.s3 = s3;
    this.configuration = configuration;
    this.s3Ops = s3Ops;
  }

  public Void handleRequest(CruiseProcessingMessage snsMessage) {

    LOGGER.info("Started Event: {}", snsMessage);

    MvtEventContext eventContext = new MvtEventContext(
        configuration.getZarrBucketName(),
        snsMessage.getShipName(),
        snsMessage.getCruiseName(),
        snsMessage.getSensorName(),
        configuration.getMsSplit(),
        configuration.getBatchSize(),
        configuration.getGeoJsonPrecision(),
        configuration.getMaxZoom(),
        configuration.getMinSimplification(),
        configuration.getMaxSimplification(),
        configuration.getMaxUploadBuffers()
    );

    LOGGER.info("Context: {}", eventContext);

    String key = "spatial/mvt/cruise/" + eventContext.getShipName() + "/" + eventContext.getCruiseName() + "/" + eventContext.getSensorName();
    s3Ops.deleteObjects(configuration.getZarrBucketName(), s3Ops.listObjects(configuration.getZarrBucketName(), key + "/"));

    ZarrToGeoJsonPipe zarrToGeoJsonPipe = new ZarrToGeoJsonPipe(s3, eventContext, OBJECT_MAPPER, TheGeometryFactory.GEOMETRY_FACTORY);
    MvtStore mvtStore = new AwsS3MvtStore(s3, eventContext);
    GeoJsonToMvtPipe geoJsonToMvtPipe = new GeoJsonToMvtPipe(OBJECT_MAPPER, TheGeometryFactory.GEOMETRY_FACTORY, eventContext,
        mvtStore);
    geoJsonToMvtPipe.pipe(zarrToGeoJsonPipe::pipe);

    LOGGER.info("Finished Event: {}", snsMessage);

    return null;
  }
}
