package edu.colorado.cires.cmg.echofish.aws.lambda.mvt.simplification;

import edu.colorado.cires.cmg.echofish.aws.lambda.mvt.zarr.DataPointZarrIterator;
import java.util.Iterator;

public class DataPointRowIterator implements Iterator<DataPointRow> {

  private final DataPointZarrIterator dataPointZarrIterator;

  public DataPointRowIterator(DataPointZarrIterator dataPointZarrIterator) {
    this.dataPointZarrIterator = dataPointZarrIterator;
  }

  @Override
  public boolean hasNext() {
    return dataPointZarrIterator.hasNext();
  }

  @Override
  public DataPointRow next() {
    return new DataPointRow(dataPointZarrIterator.next());
  }
}
