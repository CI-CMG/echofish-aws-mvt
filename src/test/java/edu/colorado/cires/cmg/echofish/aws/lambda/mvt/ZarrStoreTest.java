package edu.colorado.cires.cmg.echofish.aws.lambda.mvt;

import com.bc.zarr.ZarrArray;
import com.bc.zarr.ZarrGroup;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.colorado.cires.cmg.awszarr.AwsS3ClientWrapper;
import edu.colorado.cires.cmg.awszarr.AwsS3ZarrStore;
import edu.colorado.cires.cmg.echofish.aws.lambda.mvt.mvt.GeoJsonGeometrySource;
import edu.colorado.cires.cmg.echofish.aws.lambda.mvt.simplification.DataPointRowTracklineProcessor;
import edu.colorado.cires.cmg.echofish.aws.lambda.mvt.zarr.DataPoint;
import edu.colorado.cires.cmg.echofish.aws.lambda.mvt.zarr.DataPointGrouper;
import edu.colorado.cires.cmg.echofish.aws.lambda.mvt.zarr.DataPointZarrIterator;
import edu.colorado.cires.cmg.iostream.Pipe;
import edu.colorado.cires.cmg.mvtset.GeometryDetails;
import edu.colorado.cires.cmg.mvtset.GeometrySource;
import edu.colorado.cires.cmg.mvtset.MvtSetGenerator;
import edu.colorado.cires.cmg.mvtset.MvtStore;
import edu.colorado.cires.cmg.tracklinegen.GeoJsonMultiLineProcessor;
import edu.colorado.cires.cmg.tracklinegen.GeometrySimplifier;
import edu.colorado.cires.cmg.tracklinegen.ValidationException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

@Disabled
public class ZarrStoreTest {


//  private static final Logger LOGGER = LoggerFactory.getLogger(ZarrStoreTest.class);
//
//    private static final String s3BucketName = "ef-demo-zarr-store";
////  private static final String zarrBucketName = "echofish-dev-master-118234403147-echofish-zarr-store";
//
//  private static final int MAX_ZOOM = 5;
//  private static final double MIN_SIMPLIFICATION = 0.00001;
//  private static final double MAX_SIMPLIFICATION = 0.01;
//
//  @Test
//  public void testSimplification() throws Exception {
//
//    System.setProperty("aws.profile", "echofish");
//
//    System.setProperty("aws.region", "us-west-2");
//
//    final String s3BucketZarrKey = "AL0502_resample.zarr";
//    final ObjectMapper objectMapper = new ObjectMapper();
//
//    final long msSplit = 1000 * 60 * 60 * 2; //2 hours
//    final int batchSize = DataPointZarrIterator.DEFAULT_BUFFER_SIZE;
//    final GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);
//    final int geoJsonPrecision = 5;
//    final double simplificationTolerance = 0.0001;
//
//    final Path outputFile = Paths.get("target/simplified.json");
//
//    try (S3Client s3 = S3Client.builder().build()) {
//
//      Consumer<OutputStream> geoJsonOutWriter = out -> {
//        try {
//          new DataPointRowTracklineProcessor(
//              s3,
//              s3BucketName,
//              s3BucketZarrKey,
//              objectMapper,
//              out,
//              msSplit,
//              new GeometrySimplifier(simplificationTolerance),
//              batchSize,
//              geometryFactory,
//              geoJsonPrecision).process();
//        } catch (IOException e) {
//          throw new IllegalStateException("Unable to process zarr data", e);
//        } finally {
//          try {
//            out.close();
//          } catch (Exception e) {
//            throw new IllegalStateException("Unable to process zarr data", e);
//          }
//        }
//      };
//
//      Files.createDirectories(outputFile.getParent());
//
//      try (OutputStream fileOut = Files.newOutputStream(outputFile)) {
//        Pipe.pipe(geoJsonOutWriter, in -> {
//          try {
//            GeoJsonMultiLineProcessor phase2 = new GeoJsonMultiLineProcessor(
//                objectMapper,
//                geoJsonPrecision,
//                0
//            );
//            phase2.process(in, fileOut, new OutputStream() {
//              @Override
//              public void write(int b) throws IOException {
//                //no-op
//              }
//            });
//          } catch (ValidationException e) {
//            throw new IllegalStateException("Unable to generate geometry", e);
//          }
//        });
//      }
//
//    }
//  }
//
//
//  @Test
//  public void testGenerateMvtFromGeoJson() throws Exception {
//
//
//      Path jsonFile = Paths.get("target/simplified.json");
//      Path basePath = Paths.get("target/mvt");
//
//      int maxBufferPoints = 100;
//      GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);
//      ObjectMapper objectMapper = new ObjectMapper();
//
//      MvtStore mvtStore = new MvtStore() {
//        @Override
//        public byte[] getMvt(String index) {
//          Path path = basePath.resolve(index + ".pbf");
//          if (Files.exists(path)) {
//            try {
//              return Files.readAllBytes(path);
//            } catch (IOException e) {
//              throw new IllegalStateException("Unable to read file", e);
//            }
//          }
//          return null;
//        }
//
//        @Override
//        public void saveMvt(String index, byte[] mvtBytes) {
//          Path path = basePath.resolve(index + ".pbf");
//          Path parent = path.getParent();
//          try {
//            Files.createDirectories(parent);
//            try (OutputStream outputStream = Files.newOutputStream(path)) {
//              outputStream.write(mvtBytes);
//            }
//          } catch (IOException e) {
//            throw new IllegalStateException("Unable to write file", e);
//          }
//        }
//
//        @Override
//        public void clearStore() {
//          throw new UnsupportedOperationException();
//        }
//      };
//
//      final String cruise = "AL0502";
//      Map<String, Object> props = new HashMap<>();
//      props.put("cruise", cruise);
//
//      try(InputStream inputStream = Files.newInputStream(jsonFile)) {
//        GeometrySource geometrySource = new GeoJsonGeometrySource(inputStream, objectMapper, maxBufferPoints, geometryFactory);
//        MvtSetGenerator mvtService = new MvtSetGenerator(MAX_ZOOM, MIN_SIMPLIFICATION, MAX_SIMPLIFICATION, mvtStore, geometrySource);
//        mvtService.updateTilePyramidLayer("trackline");
//      }
//
//
//
//  }
//
//  private void readAllChunks(int chunkSize, int bufferSize, int totalCount, int readCount, ZarrArray zarrArray) {
//    int toRead = Math.min(bufferSize, totalCount - readCount);
//    int offset = readCount;
//    while (toRead > 0) {
//      int readSize = Math.min(chunkSize, toRead);
//      try {
//        zarrArray.read(new int[]{readSize}, new int[]{offset});
//      } catch (Exception e) {
//        throw new RuntimeException(e);
//      }
//      offset = offset + readSize;
//      toRead = toRead - readSize;
//    }
//  }
//
//  private void read(ZarrArray zarrArray, int readSize, int offset) {
//    try {
//      zarrArray.readConcurrently(new int[]{readSize}, new int[]{offset}, ForkJoinPool.commonPool());
//    } catch (Exception e) {
//      throw new RuntimeException(e);
//    }
//  }
//
//  @Test
//  public void testReadAll() throws Exception {
//
//    System.setProperty("aws.profile", "echofish");
//
//    System.setProperty("aws.region", "us-west-2");
//
//    long st = System.currentTimeMillis();
//
//    try (S3Client s3 = S3Client.builder().build()) {
//      ZarrGroup zarrGroup = ZarrGroup.open(
//          AwsS3ZarrStore.builder()
//              .s3(AwsS3ClientWrapper.builder().s3(s3).build())
//              .bucket(s3BucketName)
//              .key("AL0502_resample.zarr")
//              .build());
//
//      ZarrArray latitudeArray = zarrGroup.openArray("latitude");
//      ZarrArray longitudeArray = zarrGroup.openArray("longitude");
//      ZarrArray timeArray = zarrGroup.openArray("time");
//      int count = latitudeArray.getShape()[0];
//      int readCount = 0;
//      System.out.println("Start");
//      int bufferSize = DataPointZarrIterator.DEFAULT_BUFFER_SIZE;
//      while (readCount < count) {
//        int readSize = Math.min(bufferSize, count - readCount);
//        int rc = readCount;
//        long start = System.currentTimeMillis();
//        read(latitudeArray, readSize, rc);
//        read(longitudeArray, readSize, rc);
//        read(timeArray, readSize, rc);
//        long end = System.currentTimeMillis();
//        System.out.println("Time " + (end - start));
//        readCount = readCount + readSize;
//        System.out.println(readCount);
//      }
//    }
//
//    long e = System.currentTimeMillis();
//    System.out.println("Total Time " + (e - st));
//
//  }
//
//  @Test
//  public void testOpen() throws Exception {
//
//    try (S3Client s3 = S3Client.builder().build()) {
//
//      Path basePath = Paths.get("target/mvt");
//
//      MvtStore mvtStore = new MvtStore() {
//        @Override
//        public byte[] getMvt(String index) {
//          Path path = basePath.resolve(index + ".pbf");
//          if (Files.exists(path)) {
//            try {
//              return Files.readAllBytes(path);
//            } catch (IOException e) {
//              throw new IllegalStateException("Unable to read file", e);
//            }
//          }
//          return null;
//        }
//
//        @Override
//        public void saveMvt(String index, byte[] mvtBytes) {
//          Path path = basePath.resolve(index + ".pbf");
//          Path parent = path.getParent();
//          try {
//            Files.createDirectories(parent);
//            try (OutputStream outputStream = Files.newOutputStream(path)) {
//              outputStream.write(mvtBytes);
//            }
//          } catch (IOException e) {
//            throw new IllegalStateException("Unable to write file", e);
//          }
//        }
//
//        @Override
//        public void clearStore() {
//          throw new UnsupportedOperationException();
//        }
//      };
//
//      final String cruise = "AL0502";
//      Map<String, Object> props = new HashMap<>();
//      props.put("cruise", cruise);
//      final Map<String, Object> properties = Collections.unmodifiableMap(props);
//
//      GeometrySource geometrySource = new GeometrySource() {
//
//        private Coordinate toCoordinate(DataPoint point) {
//          return new Coordinate(point.longitude(), point.latitude());
//        }
//
//        @Override
//        public Stream<GeometryDetails> streamGeometries() {
//
//          DataPointZarrIterator iterator;
//          try {
//            iterator = new DataPointZarrIterator(s3, s3BucketName, "AL0502_resample.zarr");
//          } catch (IOException e) {
//            throw new RuntimeException("Unable to open zarr store", e);
//          }
//          return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new DataPointGrouper(iterator), Spliterator.ORDERED), false)
//              .map(dataPoints -> {
//                Geometry geometry;
//                if (dataPoints.size() == 1) {
//                  DataPoint point = dataPoints.get(0);
//                  geometry = MvtSetGenerator.GEOMETRY_FACTORY.createPoint(toCoordinate(point));
//                } else {
//                  geometry = MvtSetGenerator.GEOMETRY_FACTORY.createLineString(
//                      dataPoints.stream().map(this::toCoordinate).collect(Collectors.toList()).toArray(new Coordinate[0]));
//                }
//                return new GeometryDetails(geometry, properties);
//              });
//        }
//      };
//
//      System.setProperty("aws.profile", "echofish");
//
//      System.setProperty("aws.region", "us-west-2");
//
//      MvtSetGenerator mvtService = new MvtSetGenerator(MAX_ZOOM, MAX_SIMPLIFICATION, MIN_SIMPLIFICATION, mvtStore, geometrySource);
//      mvtService.updateTilePyramidLayer("trackline");
//    }
//
//  }

}