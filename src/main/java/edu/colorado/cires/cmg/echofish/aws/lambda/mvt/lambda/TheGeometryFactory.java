package edu.colorado.cires.cmg.echofish.aws.lambda.mvt.lambda;

import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;

public class TheGeometryFactory {

  public static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory(new PrecisionModel(), 4326);

}
