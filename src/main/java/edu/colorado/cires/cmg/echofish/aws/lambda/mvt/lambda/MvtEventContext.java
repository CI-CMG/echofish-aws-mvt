package edu.colorado.cires.cmg.echofish.aws.lambda.mvt.lambda;

import java.util.Objects;

public class MvtEventContext {

  private final String s3BucketName;
  private final String shipName;
  private final String cruiseName;
  private final String sensorName;
  private final long msSplit;
  private final int batchSize;
  private final int geoJsonPrecision;
  private final int maxZoom;
  private final double minSimplification;
  private final double maxSimplification;
  private final int maxUploadBuffers;

  public MvtEventContext(String s3BucketName, String shipName, String cruiseName, String sensorName, long msSplit, int batchSize, int geoJsonPrecision, int maxZoom,
      double minSimplification, double maxSimplification, int maxUploadBuffers) {
    this.s3BucketName = s3BucketName;
    this.shipName = shipName;
    this.cruiseName = cruiseName;
    this.sensorName = sensorName;
    this.msSplit = msSplit;
    this.batchSize = batchSize;
    this.geoJsonPrecision = geoJsonPrecision;
    this.maxZoom = maxZoom;
    this.minSimplification = minSimplification;
    this.maxSimplification = maxSimplification;
    this.maxUploadBuffers = maxUploadBuffers;
  }

  public String getS3BucketName() {
    return s3BucketName;
  }

  public String getCruiseName() {
    return cruiseName;
  }

  public String getShipName() {
    return shipName;
  }

  public String getSensorName() {
    return sensorName;
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
    MvtEventContext that = (MvtEventContext) o;
    return msSplit == that.msSplit && batchSize == that.batchSize && geoJsonPrecision == that.geoJsonPrecision && maxZoom == that.maxZoom
        && Double.compare(that.minSimplification, minSimplification) == 0
        && Double.compare(that.maxSimplification, maxSimplification) == 0 && maxUploadBuffers == that.maxUploadBuffers
        && Objects.equals(s3BucketName, that.s3BucketName) && Objects.equals(shipName, that.shipName) && Objects.equals(
        cruiseName, that.cruiseName) && Objects.equals(sensorName, that.sensorName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(s3BucketName, shipName, cruiseName, sensorName, msSplit, batchSize, geoJsonPrecision, maxZoom, minSimplification,
        maxSimplification, maxUploadBuffers);
  }

  @Override
  public String toString() {
    return "MvtEventContext{" +
        "s3BucketName='" + s3BucketName + '\'' +
        ", shipName='" + shipName + '\'' +
        ", cruiseName='" + cruiseName + '\'' +
        ", sensorName='" + sensorName + '\'' +
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
