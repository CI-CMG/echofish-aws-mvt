package edu.colorado.cires.cmg.echofish.aws.lambda.mvt.zarr;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class DataPointGrouper implements Iterator<List<DataPoint>> {

  private static final int DEFAULT_GROUP_SIZE = 8000;

  private final int groupSize;
  private final DataPointZarrIterator dataPointZarrIterator;

  public DataPointGrouper(DataPointZarrIterator dataPointZarrIterator) {
    this(dataPointZarrIterator, DEFAULT_GROUP_SIZE);
  }

  public DataPointGrouper(DataPointZarrIterator dataPointZarrIterator, int bufferSize) {
    this.groupSize = bufferSize;
    this.dataPointZarrIterator = dataPointZarrIterator;
  }

  private List<DataPoint> read() {
    int readCount = 0;
    List<DataPoint> points = new ArrayList<>(groupSize);
    while (dataPointZarrIterator.hasNext() && readCount < groupSize) {
      DataPoint point = dataPointZarrIterator.next();
      points.add(point);
      readCount++;
    }
    return points;
  }


  @Override
  public boolean hasNext() {
    return dataPointZarrIterator.hasNext();
  }

  @Override
  public List<DataPoint> next() {
    List<DataPoint> points = read();
    if (points.isEmpty()) {
      throw new NoSuchElementException("No more data");
    }
    return points;
  }
}
