package edu.colorado.cires.cmg.echofish.aws.lambda.mvt.mvt;

import edu.colorado.cires.cmg.awszarr.S3ClientWrapper;
import edu.colorado.cires.cmg.echofish.aws.lambda.mvt.lambda.MvtMergeEventContext;
import edu.colorado.cires.cmg.mvtset.MvtStore;
import edu.colorado.cires.cmg.s3out.S3OutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3MvtStore implements MvtStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3MvtStore.class);

  private final S3ClientWrapper s3;
  private final MvtMergeEventContext eventContext;
  private final BiFunction<MvtMergeEventContext, String, String> getKey;
  private final Function<MvtMergeEventContext, String> getKeyRoot;

  public S3MvtStore(S3ClientWrapper s3, MvtMergeEventContext eventContext, BiFunction<MvtMergeEventContext, String, String> getKey,
      Function<MvtMergeEventContext, String> getKeyRoot) {
    this.s3 = s3;
    this.eventContext = eventContext;
    this.getKey = getKey;
    this.getKeyRoot = getKeyRoot;
  }

  private String getBucket() {
    return eventContext.getS3BucketName();
  }

  private int getQueueSize() {
    return eventContext.getMaxUploadBuffers();
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

  @Override
  public byte[] getMvt(String index) {
    final Optional<InputStream> maybeIn = s3.getObject(getBucket(), getKey.apply(eventContext, index));
    try {
      return maybeIn.map(this::toByteArray).orElseGet(() -> new byte[0]);
    } finally {
      maybeIn.ifPresent(this::close);
    }
  }

  @Override
  public void saveMvt(String index, byte[] mvtBytes) {
    try (S3OutputStream out = S3OutputStream.builder()
        .s3(s3)
        .autoComplete(false)
        .bucket(getBucket())
        .key(getKey.apply(eventContext, index))
        .uploadQueueSize(getQueueSize())
        .build()
    ) {
      IOUtils.write(mvtBytes, out);
      out.done();
    } catch (IOException e) {
      throw new RuntimeException("Unable to write MVT", e);
    }
  }

  @Override
  public void clearStore() {
    throw new UnsupportedOperationException("Clear store is not supported");
  }

  @Override
  public List<String> listIndexes() {
    List<String> indexes = new ArrayList<>();
    try (Stream<ListObjectsV2Response> stream = s3.listObjectsV2Paginator(eventContext.getS3BucketName(), getKeyRoot.apply(eventContext))) {
      stream.forEach((resp) -> {
        for (S3Object s3Obj : resp.contents()) {
          String key = s3Obj.key();
          String[] parts = key.split("/");
          indexes.add(String.format("%s/%s/%s", parts[parts.length - 3], parts[parts.length - 2], parts[parts.length - 1].replaceAll("\\.pbf", "")));
        }
      });
    }
    return indexes;
  }

}
