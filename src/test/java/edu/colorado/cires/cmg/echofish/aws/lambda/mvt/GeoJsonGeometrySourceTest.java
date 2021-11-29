package edu.colorado.cires.cmg.echofish.aws.lambda.mvt;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.colorado.cires.cmg.echofish.aws.lambda.mvt.mvt.GeoJsonGeometrySource;
import edu.colorado.cires.cmg.mvtset.GeometryDetails;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.PrecisionModel;

@Disabled
public class GeoJsonGeometrySourceTest {

//  @Test
//  public void test() throws Exception {
//    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);
//    ObjectMapper objectMapper = new ObjectMapper();
//
//    Path filePath = Paths.get("target/simplified.json");
//    try (InputStream in = Files.newInputStream(filePath)) {
//
//      int maxBufferPoints = 100;
//      GeoJsonGeometrySource geometrySource = new GeoJsonGeometrySource(in, objectMapper, maxBufferPoints, geometryFactory);
//      List<LineString> lineStrings;
//      try (Stream<GeometryDetails> stream = geometrySource.streamGeometries()) {
//        lineStrings = stream.map(geometryDetails -> (LineString) geometryDetails.getGeometry()).collect(Collectors.toList());
//      }
//      System.out.println(lineStrings.size());
//    }
//  }
}