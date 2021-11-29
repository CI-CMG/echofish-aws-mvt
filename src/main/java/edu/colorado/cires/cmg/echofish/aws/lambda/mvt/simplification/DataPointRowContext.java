package edu.colorado.cires.cmg.echofish.aws.lambda.mvt.simplification;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.colorado.cires.cmg.awszarr.S3ClientWrapper;
import edu.colorado.cires.cmg.echofish.aws.lambda.mvt.zarr.DataPointZarrIterator;
import edu.colorado.cires.cmg.tracklinegen.GeoJsonMultiLineWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataPointRowContext implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataPointRowContext.class);

  private final DataPointZarrIterator dataPointZarrIterator;
  private final JsonGenerator jsonGenerator;
  private final GeoJsonMultiLineWriter lineWriter;

  public DataPointRowContext(
      S3ClientWrapper s3,
      String s3BucketName,
      String s3BucketZarrKey,
      int precision,
      ObjectMapper objectMapper,
      OutputStream out) {
    try {
      dataPointZarrIterator = new DataPointZarrIterator(s3, s3BucketName, s3BucketZarrKey);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to open Zarr store", e);
    }
    try {
      jsonGenerator = objectMapper.getFactory().createGenerator(out);
    } catch (IOException e) {
      throw new RuntimeException("Unable to create JSON generator", e);
    }
    lineWriter = new GeoJsonMultiLineWriter(jsonGenerator, precision);
  }

  public DataPointZarrIterator getDataPointZarrIterator() {
    return dataPointZarrIterator;
  }

  public GeoJsonMultiLineWriter getLineWriter() {
    return lineWriter;
  }

  @Override
  public void close() throws IOException {
    List<Exception> listExceptions = new ArrayList<>();
    try {
      jsonGenerator.close();
    } catch (Exception e) {
      LOGGER.warn("Unable to close JSON generator", e);
      listExceptions.add(e);
    }

    if (!listExceptions.isEmpty()) {
      throw new IllegalStateException("Trackline context problem", listExceptions.get(0));
    }

  }
}
