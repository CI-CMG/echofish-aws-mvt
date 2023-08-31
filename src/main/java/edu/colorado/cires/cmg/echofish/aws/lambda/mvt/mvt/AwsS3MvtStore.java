package edu.colorado.cires.cmg.echofish.aws.lambda.mvt.mvt;

import edu.colorado.cires.cmg.awszarr.S3ClientWrapper;
import edu.colorado.cires.cmg.echofish.aws.lambda.mvt.lambda.MvtEventContext;
import edu.colorado.cires.cmg.mvtset.MvtStore;
import edu.colorado.cires.cmg.s3out.S3OutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

public class AwsS3MvtStore implements MvtStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(AwsS3MvtStore.class);

  private final S3ClientWrapper s3;
  private final MvtEventContext eventContext;

  public AwsS3MvtStore(S3ClientWrapper s3, MvtEventContext eventContext) {
    this.s3 = s3;
    this.eventContext = eventContext;
  }

  private void close(InputStream in) {
    try {
      in.close();
    } catch (IOException e) {
      LOGGER.warn("Unable to close input stream", e);
    }
  }

  private byte[] toByteArray(InputStream in) {
    try {
      return IOUtils.toByteArray(in);
    } catch (IOException e) {
      throw new RuntimeException("Unable to read mvt", e);
    }
  }

  private String getKey(String index) {
    return "spatial/mvt/cruise/" + eventContext.getShipName() + "/" + eventContext.getCruiseName() + "/" + eventContext.getSensorName() + "/" + index + ".pbf";
  }

  private String getBucket() {
    return eventContext.getS3BucketName();
  }

  private int getQueueSize() {
    return eventContext.getMaxUploadBuffers();
  }

  private String getSurvey() {
    return eventContext.getCruiseName();
  }

  @Override
  public byte[] getMvt(String index) {
    final Optional<InputStream> maybeIn = s3.getObject(getBucket(), getKey(index));
    try {
      return maybeIn.map(this::toByteArray).orElseGet(() -> new byte[0]);
    } finally {
      maybeIn.ifPresent(this::close);
    }
  }

  @Override
  public void saveMvt(String index, byte[] mvtBytes) {
    try(S3OutputStream out = S3OutputStream.builder()
        .s3(s3)
        .autoComplete(false)
        .bucket(getBucket())
        .key(getKey(index))
        .uploadQueueSize(getQueueSize())
        .build()
    ) {
      IOUtils.write(mvtBytes, out);
      out.done();
    } catch (IOException e) {
      throw new RuntimeException("Unable to write MVT", e);
    }
  }

  private void delete(S3Object s3Object) {
    s3.deleteObject(getBucket(), s3Object.key());
  }

  @Override
  public void clearStore() {
    try(Stream<ListObjectsV2Response> stream = s3.listObjectsV2Paginator(getBucket(), getSurvey())) {
      stream.flatMap(response -> response.contents().stream()).forEach(this::delete);
    }
  }
}
