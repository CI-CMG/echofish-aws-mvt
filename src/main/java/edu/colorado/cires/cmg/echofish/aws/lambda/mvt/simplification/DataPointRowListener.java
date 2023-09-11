package edu.colorado.cires.cmg.echofish.aws.lambda.mvt.simplification;

import edu.colorado.cires.cmg.tracklinegen.BaseRowListener;
import edu.colorado.cires.cmg.tracklinegen.GeoJsonMultiLineWriter;
import edu.colorado.cires.cmg.tracklinegen.GeometrySimplifier;
import java.util.function.Predicate;
import org.locationtech.jts.geom.GeometryFactory;

public class DataPointRowListener extends BaseRowListener<DataPointRow> {

  private static final Predicate<DataPointRow> FILTER = row ->
      row.getLat() != null
          && row.getLon() != null
          && row.getTimestamp() != null
          && row.getTimestamp().toEpochMilli() != 0L
          && !Double.isNaN(row.getLat())
          && !Double.isNaN(row.getLon())
          && Math.abs(row.getLat() - 0D) > 0.00001
          && Math.abs(row.getLon() - 0D) > 0.00001;

  public DataPointRowListener(
      long msSplit,
      GeometrySimplifier geometrySimplifier,
      GeoJsonMultiLineWriter lineWriter,
      int batchSize,
      GeometryFactory geometryFactory,
      int geoJsonPrecision) {
    super(msSplit, geometrySimplifier, lineWriter, batchSize, FILTER, 0, geometryFactory, geoJsonPrecision);
  }
}
