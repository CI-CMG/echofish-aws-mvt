package edu.colorado.cires.cmg.echofish.aws.lambda.mvt.simplification;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.colorado.cires.cmg.awszarr.S3ClientWrapper;
import edu.colorado.cires.cmg.tracklinegen.GeometrySimplifier;
import edu.colorado.cires.cmg.tracklinegen.TracklineProcessor;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.locationtech.jts.geom.GeometryFactory;

public class DataPointRowTracklineProcessor extends TracklineProcessor<DataPointRowContext, DataPointRow, DataPointRowListener> {

  private final S3ClientWrapper s3;
  private final String s3BucketName;
  private final String s3BucketZarrKey;
  private final ObjectMapper objectMapper;
  private final OutputStream out;
  private final long msSplit;
  private final GeometrySimplifier geometrySimplifier;
  private final int batchSize;
  private final GeometryFactory geometryFactory;
  private final int geoJsonPrecision;

  public DataPointRowTracklineProcessor(
      S3ClientWrapper s3,
      String s3BucketName,
      String s3BucketZarrKey,
      ObjectMapper objectMapper,
      OutputStream out,
      long msSplit,
      GeometrySimplifier geometrySimplifier,
      int batchSize,
      GeometryFactory geometryFactory,
      int geoJsonPrecision) {
    this.s3 = s3;
    this.s3BucketName = s3BucketName;
    this.s3BucketZarrKey = s3BucketZarrKey;
    this.objectMapper = objectMapper;
    this.out = out;
    this.msSplit = msSplit;
    this.geometrySimplifier = geometrySimplifier;
    this.batchSize = batchSize;
    this.geometryFactory = geometryFactory;
    this.geoJsonPrecision = geoJsonPrecision;
  }

  @Override
  protected Iterator<DataPointRow> getRows(DataPointRowContext dataPointRowContext) {
    return new DataPointRowIterator(dataPointRowContext.getDataPointZarrIterator());
  }

  @Override
  protected List<DataPointRowListener> createRowListeners(DataPointRowContext dataPointRowContext) {
    return Collections.singletonList(new DataPointRowListener(
        msSplit,
        geometrySimplifier,
        dataPointRowContext.getLineWriter(),
        batchSize,
        geometryFactory,
        geoJsonPrecision
    ));
  }

  @Override
  protected DataPointRowContext createProcessingContext() {
    return new DataPointRowContext(s3, s3BucketName, s3BucketZarrKey, geoJsonPrecision, objectMapper, out);
  }
}
