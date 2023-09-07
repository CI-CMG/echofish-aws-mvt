package edu.colorado.cires.cmg.echofish.aws.lambda.mvt.mvt;

import edu.colorado.cires.cmg.awszarr.S3ClientWrapper;
import edu.colorado.cires.cmg.echofish.aws.lambda.mvt.lambda.MvtEventContext;
import edu.colorado.cires.cmg.mvtset.MvtStore;
import edu.colorado.cires.cmg.s3out.S3OutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AwsS3MvtStore implements MvtStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(AwsS3MvtStore.class);

  private final S3ClientWrapper s3;
  private final MvtEventContext eventContext;
  private static final Path mvtDir = Paths.get("/tmp").resolve("mvt");

  public AwsS3MvtStore(S3ClientWrapper s3, MvtEventContext eventContext) {
    this.s3 = s3;
    this.eventContext = eventContext;
    FileUtils.deleteQuietly(mvtDir.toFile());
    try {
      Files.createDirectories(mvtDir);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String getKey(String index) {
    return "spatial/mvt/cruise/" + eventContext.getShipName() + "/" + eventContext.getCruiseName() + "/" + eventContext.getSensorName() + "/" + index
        + ".pbf";
  }

  private String getBucket() {
    return eventContext.getS3BucketName();
  }

  private int getQueueSize() {
    return eventContext.getMaxUploadBuffers();
  }

  @Override
  public byte[] getMvt(String index) {
    if (Files.exists(mvtDir.resolve(index + ".pbf"))) {
      try {
        return FileUtils.readFileToByteArray(mvtDir.resolve(index + ".pbf").toFile());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return new byte[0];
  }

  @Override
  public void saveMvt(String index, byte[] mvtBytes) {
    try {
      FileUtils.writeByteArrayToFile(mvtDir.resolve(index + ".pbf").toFile(), mvtBytes);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void clearStore() {
    throw new UnsupportedOperationException("Clear store is not supported");
  }

  public void sync() {

    FileUtils.listFiles(mvtDir.toFile(), new String[]{"pbf"}, true).parallelStream().forEach(path -> {
      List<String> parts = new ArrayList<>();
      Iterator<Path> it = path.toPath().toAbsolutePath().normalize().iterator();
      while (it.hasNext()) {
        parts.add(it.next().toString().replaceAll("\\.pbf$", ""));
      }
      String index = String.join("/", parts.subList(parts.size() - 3, parts.size()));
      String key = getKey(index);
      try (S3OutputStream out = S3OutputStream.builder()
          .s3(s3)
          .autoComplete(false)
          .bucket(getBucket())
          .key(key)
          .uploadQueueSize(getQueueSize())
          .build()
      ) {
        LOGGER.info("writing to S3 {}", key);
        IOUtils.write(FileUtils.readFileToByteArray(path), out);
        out.done();
      } catch (IOException e) {
        throw new RuntimeException("Unable to write MVT", e);
      }
    });


  }
}
