package edu.colorado.cires.cmg.echofish.aws.lambda.mvt.simplification;

import edu.colorado.cires.cmg.echofish.aws.lambda.mvt.zarr.DataPoint;
import edu.colorado.cires.cmg.tracklinegen.DataRow;
import java.time.Instant;

public class DataPointRow implements DataRow {

  private final DataPoint dataPoint;

  public DataPointRow(DataPoint dataPoint) {
    this.dataPoint = dataPoint;
  }

  @Override
  public Instant getTimestamp() {
    return Instant.ofEpochMilli(dataPoint.getTime());
  }

  @Override
  public Double getLon() {
    return dataPoint.getLongitude();
  }

  @Override
  public Double getLat() {
    return dataPoint.getLatitude();
  }
}
