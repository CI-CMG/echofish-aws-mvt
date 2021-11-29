package edu.colorado.cires.cmg.echofish.aws.lambda.mvt.zarr;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.colorado.cires.cmg.awszarr.S3ClientWrapper;
import edu.colorado.cires.cmg.echofish.aws.lambda.mvt.lambda.MvtEventContext;
import edu.colorado.cires.cmg.iostream.Pipe;
import edu.colorado.cires.cmg.tracklinegen.GeoJsonMultiLineProcessor;
import edu.colorado.cires.cmg.tracklinegen.ValidationException;
import java.io.IOException;
import java.io.OutputStream;
import org.locationtech.jts.geom.GeometryFactory;

public class ZarrToGeoJsonPipe {

  private static final double NO_SPEED_CHECK = 0D;
  private static final OutputStream NO_OP_OUT = new OutputStream() {
    @Override
    public void write(int b) throws IOException {
      //no-op
    }
  };

  private final MvtEventContext eventContext;
  private final ObjectMapper objectMapper;
  private final ZarrToGeoJsonWriter zarrToGeoJsonWriter;

  public ZarrToGeoJsonPipe(S3ClientWrapper s3, MvtEventContext eventContext, ObjectMapper objectMapper, GeometryFactory geometryFactory) {
    this.eventContext = eventContext;
    this.objectMapper = objectMapper;
    zarrToGeoJsonWriter = new ZarrToGeoJsonWriter(s3, eventContext, objectMapper, geometryFactory);
  }

  public void pipe(OutputStream geoJsonOut) {
    Pipe.pipe(zarrToGeoJsonWriter, in -> {
      try {
        GeoJsonMultiLineProcessor phase2 = new GeoJsonMultiLineProcessor(objectMapper,
            eventContext.getGeoJsonPrecision(),
            NO_SPEED_CHECK
        );
        phase2.process(in, geoJsonOut, NO_OP_OUT);
      } catch (ValidationException e) {
        throw new IllegalStateException("Unable to generate geometry", e);
      }
    });
  }
}
