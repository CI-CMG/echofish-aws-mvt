package edu.colorado.cires.cmg.echofish.aws.lambda.mvt.mvt;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.colorado.cires.cmg.echofish.aws.lambda.mvt.lambda.MvtEventContext;
import edu.colorado.cires.cmg.iostream.Pipe;
import edu.colorado.cires.cmg.mvtset.MvtSetGenerator;
import edu.colorado.cires.cmg.mvtset.MvtStore;
import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Consumer;
import org.locationtech.jts.geom.GeometryFactory;

public class GeoJsonToMvtPipe {

  private final ObjectMapper objectMapper;
  private final GeometryFactory geometryFactory;
  private final MvtEventContext eventContext;
  private final MvtStore mvtStore;

  public GeoJsonToMvtPipe(
      ObjectMapper objectMapper,
      GeometryFactory geometryFactory,
      MvtEventContext eventContext,
      MvtStore mvtStore) {
    this.objectMapper = objectMapper;
    this.geometryFactory = geometryFactory;
    this.eventContext = eventContext;
    this.mvtStore = mvtStore;
  }


  public void pipe(Consumer<OutputStream> outWriter) {

    Pipe.pipe(outWriter, in -> {
      try (GeoJsonGeometrySource geometrySource = new GeoJsonGeometrySource(in, objectMapper, eventContext.getBatchSize(), geometryFactory, eventContext)) {
        MvtSetGenerator mvtService = new MvtSetGenerator(
            eventContext.getMaxZoom(),
            eventContext.getMaxSimplification(),
            eventContext.getMinSimplification(),
            mvtStore,
            geometrySource);
        mvtService.updateTilePyramidLayer(String.format("%s_%s_%s", eventContext.getShipName(), eventContext.getCruiseName(), eventContext.getSensorName()));
      } catch (IOException e) {
        throw new IllegalStateException("Unable to open geometry source", e);
      }
    });
  }

}
