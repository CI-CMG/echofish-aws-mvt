package edu.colorado.cires.cmg.echofish.aws.lambda.mvt.lambda;

import java.util.Objects;

public class MvtLambdaConfiguration {

  private final String zarrBucketName;
  private final long msSplit;
  private final int batchSize;
  private final int geoJsonPrecision;
  private final int maxZoom;
  private final double minSimplification;
  private final double maxSimplification;
  private final int maxUploadBuffers;


  public MvtLambdaConfiguration(String zarrBucketName, long msSplit, int batchSize, int geoJsonPrecision, int maxZoom, double minSimplification,
      double maxSimplification, int maxUploadBuffers) {
    this.zarrBucketName = zarrBucketName;
    this.msSplit = msSplit;
    this.batchSize = batchSize;
    this.geoJsonPrecision = geoJsonPrecision;
    this.maxZoom = maxZoom;
    this.minSimplification = minSimplification;
    this.maxSimplification = maxSimplification;
    this.maxUploadBuffers = maxUploadBuffers;
  }

  public String getZarrBucketName() {
    return zarrBucketName;
  }

  public long getMsSplit() {
    return msSplit;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public int getGeoJsonPrecision() {
    return geoJsonPrecision;
  }

  public int getMaxZoom() {
    return maxZoom;
  }

  public double getMinSimplification() {
    return minSimplification;
  }

  public double getMaxSimplification() {
    return maxSimplification;
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
    MvtLambdaConfiguration that = (MvtLambdaConfiguration) o;
    return msSplit == that.msSplit && batchSize == that.batchSize && geoJsonPrecision == that.geoJsonPrecision && maxZoom == that.maxZoom
        && Double.compare(that.minSimplification, minSimplification) == 0
        && Double.compare(that.maxSimplification, maxSimplification) == 0 && maxUploadBuffers == that.maxUploadBuffers
        && Objects.equals(zarrBucketName, that.zarrBucketName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(zarrBucketName, msSplit, batchSize, geoJsonPrecision, maxZoom, minSimplification, maxSimplification,
        maxUploadBuffers);
  }

  @Override
  public String toString() {
    return "MvtLambdaConfiguration{" +
        "zarrBucketName='" + zarrBucketName + '\'' +
        ", msSplit=" + msSplit +
        ", batchSize=" + batchSize +
        ", geoJsonPrecision=" + geoJsonPrecision +
        ", maxZoom=" + maxZoom +
        ", minSimplification=" + minSimplification +
        ", maxSimplification=" + maxSimplification +
        ", maxUploadBuffers=" + maxUploadBuffers +
        '}';
  }
}
