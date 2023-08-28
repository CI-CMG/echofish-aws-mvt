package edu.colorado.cires.cmg.echofish.aws.lambda.mvt.zarr;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.colorado.cires.cmg.awszarr.S3ClientWrapper;
import edu.colorado.cires.cmg.echofish.aws.lambda.mvt.lambda.MvtEventContext;
import edu.colorado.cires.cmg.echofish.aws.lambda.mvt.simplification.DataPointRowTracklineProcessor;
import edu.colorado.cires.cmg.tracklinegen.GeometrySimplifier;
import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Consumer;
import org.locationtech.jts.geom.GeometryFactory;

public class ZarrToGeoJsonWriter implements Consumer<OutputStream> {

  private final S3ClientWrapper s3;
  private final MvtEventContext eventContext;
  private final ObjectMapper objectMapper;
  private final GeometryFactory geometryFactory;

  public ZarrToGeoJsonWriter(S3ClientWrapper s3, MvtEventContext eventContext, ObjectMapper objectMapper,
      GeometryFactory geometryFactory) {
    this.s3 = s3;
    this.eventContext = eventContext;
    this.objectMapper = objectMapper;
    this.geometryFactory = geometryFactory;
  }

  @Override
  public void accept(OutputStream out) {
    try {
      new DataPointRowTracklineProcessor(
          s3,
          eventContext.getS3BucketName(),
          "level_2/" + eventContext.getShipName() + "/" + eventContext.getCruiseName() + "/" + eventContext.getSensorName() + "/" + eventContext.getCruiseName() + ".zarr",
          objectMapper,
          out,
          eventContext.getMsSplit(),
          new GeometrySimplifier(eventContext.getMinSimplification()),
          eventContext.getBatchSize(),
          geometryFactory,
          eventContext.getGeoJsonPrecision()).process();
    } catch (IOException e) {
      throw new IllegalStateException("Unable to process zarr data", e);
    } finally {
      try {
        out.close();
      } catch (Exception e) {
        throw new IllegalStateException("Unable to process zarr data", e);
      }
    }
  }
}
