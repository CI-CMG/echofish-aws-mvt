package edu.colorado.cires.cmg.echofish.aws.lambda.mvt.lambda;

import java.util.Objects;

public class MvtMergeLambdaConfiguration {

  private final String zarrBucketName;
  private final int maxUploadBuffers;


  public MvtMergeLambdaConfiguration(String zarrBucketName, int maxUploadBuffers) {
    this.zarrBucketName = zarrBucketName;
    this.maxUploadBuffers = maxUploadBuffers;
  }

  public String getZarrBucketName() {
    return zarrBucketName;
  }

  public int getMaxUploadBuffers() {
    return maxUploadBuffers;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MvtMergeLambdaConfiguration that = (MvtMergeLambdaConfiguration) o;
    return maxUploadBuffers == that.maxUploadBuffers && Objects.equals(zarrBucketName, that.zarrBucketName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(zarrBucketName, maxUploadBuffers);
  }

  @Override
  public String toString() {
    return "MvtMergeLambdaConfiguration{" +
        "zarrBucketName='" + zarrBucketName + '\'' +
        ", maxUploadBuffers=" + maxUploadBuffers +
        '}';
  }
}
