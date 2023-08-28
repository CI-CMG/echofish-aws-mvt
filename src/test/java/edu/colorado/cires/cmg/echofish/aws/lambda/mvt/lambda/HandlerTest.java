package edu.colorado.cires.cmg.echofish.aws.lambda.mvt.lambda;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.bc.zarr.ArrayParams;
import com.bc.zarr.DataType;
import com.bc.zarr.ZarrArray;
import com.bc.zarr.ZarrGroup;
import edu.colorado.cires.cmg.awszarr.FileMockS3ClientWrapper;
import edu.colorado.cires.cmg.echofish.aws.test.MockS3Operations;
import edu.colorado.cires.cmg.echofish.aws.test.S3TestUtils;
import edu.colorado.cires.cmg.echofish.data.model.CruiseProcessingMessage;
import edu.colorado.cires.cmg.echofish.data.s3.S3Operations;
import edu.colorado.cires.cmg.echofish.data.sns.SnsNotifier;
import edu.colorado.cires.cmg.echofish.data.sns.SnsNotifierFactory;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HandlerTest {

    private static final String TABLE_NAME = "FILE_INFO";
    private static final Path BUCKET_BASE = Paths.get("target/output").toAbsolutePath();
    private static final String ZARR_BUCKET = "zarr-bucket";
    private static final String MVT_BUCKET = "mvt-bucket";
    private static final String TOPIC_ARN = "done-topic";

//    private AmazonDynamoDB dynamo;
    private MvtLambdaHandler handler;
    private SnsNotifierFactory sns;
//    private DynamoDBMapper mapper;

    private static class CustomMockS3Operations implements S3Operations {

        private final MockS3Operations s3Operations = new MockS3Operations();

        @Override
        public void copyObject(String sourceBucket, String sourceKey, String targetBucket, String targetKey) {
            s3Operations.copyObject(BUCKET_BASE.resolve(sourceBucket).toString(), sourceKey, BUCKET_BASE.resolve(targetBucket).toString(), targetKey);
        }

        @Override
        public List<String> deleteObjects(String bucket, List<String> keys) {
            return s3Operations.deleteObjects(BUCKET_BASE.resolve(bucket).toString(), keys);
        }

        @Override
        public boolean doesObjectExist(String bucket, String key) {
            return s3Operations.doesObjectExist(BUCKET_BASE.resolve(bucket).toString(), key);
        }

        @Override
        public List<String> listObjects(String bucket, String prefix) {
            return s3Operations.listObjects(BUCKET_BASE.resolve(bucket).toString(), prefix);
        }

        @Override
        public List<String> listObjects(String bucket) {
            return s3Operations.listObjects(BUCKET_BASE.resolve(bucket).toString());
        }

        @Override
        public void uploadDirectoryToBucket(Path dir, String bucket) {
            s3Operations.uploadDirectoryToBucket(dir, BUCKET_BASE.resolve(bucket).toString());
        }

        @Override
        public void upload(Path source, String targetBucket, String targetKey) {
            s3Operations.upload(source, BUCKET_BASE.resolve(targetBucket).toString(), targetKey);
        }

        @Override
        public void download(String sourceBucket, String sourceKey, Path target) {
            s3Operations.download(BUCKET_BASE.resolve(sourceBucket).toString(), sourceKey, target);
        }
    }

    private static final long splitMs = 900000L;
    private static final int batchSize = 10000;
    private final int geoJsonPrecision = 5;
    private final int maxZoom = 5;
    private final double minSimplification = 0.00001;
    private final double maxSimplification = 0.1;
    private final int maxUploadBuffers = 3;


    @BeforeEach
    public void before() throws Exception {
//        System.setProperty("sqlite4java.library.path", "native-libs");
        S3TestUtils.cleanupMockS3Directory(BUCKET_BASE);
        Files.createDirectories(BUCKET_BASE.resolve(ZARR_BUCKET));
        Files.createDirectories(BUCKET_BASE.resolve(MVT_BUCKET));
        FileUtils.copyDirectory(
            new File("src/test/resources/echofish-dev-output/level_2/Henry_B._Bigelow/HB0707/EK60/HB0707_2.zarr"),
            BUCKET_BASE.resolve(ZARR_BUCKET).resolve("level_2/Henry_B._Bigelow/HB0707/EK60/HB0707.zarr").toFile()
            );
//        dynamo = DynamoDBEmbedded.create().amazonDynamoDB();
//        mapper = new DynamoDBMapper(dynamo);
        sns = mock(SnsNotifierFactory.class);

        handler = new MvtLambdaHandler(
            FileMockS3ClientWrapper.builder().mockBucketDir(BUCKET_BASE).build(),
            new MvtLambdaConfiguration(
                ZARR_BUCKET,
                splitMs,
                batchSize,
                geoJsonPrecision,
                maxZoom,
                minSimplification,
                maxSimplification,
                MVT_BUCKET,
                maxUploadBuffers
            ));

    }

    @AfterEach
    public void after() throws Exception {
        S3TestUtils.cleanupMockS3Directory(BUCKET_BASE);
//        dynamo.shutdown();
    }

//    private enum DynamoDbHeader {
//        FILE_NAME,
//        CRUISE_NAME,
//        CHANNELS,
//        END_TIME,
//        ERROR_DETAIL,
//        ERROR_MESSAGE,
//        FREQUENCIES,
//        MAX_ECHO_RANGE,
//        MIN_ECHO_RANGE,
//        NUM_PING_TIME_DROPNA,
//        PIPELINE_STATUS,
//        PIPELINE_TIME,
//        SENSOR_NAME,
//        SHIP_NAME,
//        START_TIME,
//        ZARR_BUCKET,
//        ZARR_PATH
//    }

//    private static List<String> parseList(String value) {
//        if (StringUtils.isBlank(value)) {
//            return Collections.emptyList();
//        }
//        String[] parts = value.split(",");
//        return Arrays.asList(parts).stream().map(String::trim).collect(Collectors.toList());
//    }
//
//    private static String parseString(String value) {
//        if (StringUtils.isBlank(value)) {
//            return null;
//        }
//        return value.trim();
//    }
//
//    private static Integer parseInteger(String value) {
//        if (StringUtils.isBlank(value)) {
//            return null;
//        }
//        return Integer.parseInt(value.trim());
//    }
//
//    private static Double parseDouble(String value) {
//        if (StringUtils.isBlank(value)) {
//            return null;
//        }
//        return Double.parseDouble(value.trim());
//    }

//    private static FileInfoRecord toFileInfoRecord(CSVRecord csv) {
//        FileInfoRecord record = new FileInfoRecord();
//        record.setFileName(parseString(csv.get(DynamoDbHeader.FILE_NAME)));
//        record.setCruiseName(csv.get(DynamoDbHeader.CRUISE_NAME));
//        record.setChannels(parseList(csv.get(DynamoDbHeader.CHANNELS)));
//        record.setEndTime(parseString(csv.get(DynamoDbHeader.END_TIME)));
//        record.setErrorDetail(parseString(csv.get(DynamoDbHeader.ERROR_DETAIL)));
//        record.setErrorMessage(parseString(csv.get(DynamoDbHeader.ERROR_MESSAGE)));
//        record.setFrequencies(parseList(csv.get(DynamoDbHeader.FREQUENCIES)).stream().map(Integer::parseInt).collect(Collectors.toList()));
//        record.setMaxEchoRange(parseDouble(csv.get(DynamoDbHeader.MAX_ECHO_RANGE)));
//        record.setMinEchoRange(parseDouble(csv.get(DynamoDbHeader.MIN_ECHO_RANGE)));
//        record.setNumPingTimeDropna(parseInteger(csv.get(DynamoDbHeader.NUM_PING_TIME_DROPNA)));
//        record.setPipelineStatus(parseString(csv.get(DynamoDbHeader.PIPELINE_STATUS)));
//        record.setPipelineTime(parseString(csv.get(DynamoDbHeader.PIPELINE_TIME)));
//        record.setSensorName(parseString(csv.get(DynamoDbHeader.SENSOR_NAME)));
//        record.setShipName(parseString(csv.get(DynamoDbHeader.SHIP_NAME)));
//        record.setStartTime(parseString(csv.get(DynamoDbHeader.START_TIME)));
//        record.setZarrBucket(parseString(csv.get(DynamoDbHeader.ZARR_BUCKET)));
//        record.setZarrPath(parseString(csv.get(DynamoDbHeader.ZARR_PATH)));
//        return record;
//    }
//
//    private static List<FileInfoRecord> parseCsv(Path csvFile) throws IOException {
//        List<FileInfoRecord> records = new ArrayList<>();
//        try (CSVParser parser = CSVParser.parse(Files.newInputStream(csvFile), StandardCharsets.UTF_8,
//                CSVFormat.DEFAULT.withSkipHeaderRecord().withHeader(DynamoDbHeader.class))) {
//            for (CSVRecord record : parser) {
//                records.add(toFileInfoRecord(record));
//            }
//        }
//        return records;
//    }

//    @Test
//    public void shrinkZarr() throws Exception {
//        ZarrGroup root = ZarrGroup.open("src/test/resources/echofish-dev-output/level_2/Henry_B._Bigelow/HB0707/EK60/HB0707.zarr");
//        ZarrArray time = root.openArray("time");
//        ZarrArray latitude = root.openArray("latitude");
//        ZarrArray longitude = root.openArray("longitude");
//
//        ZarrGroup copy = ZarrGroup.create("src/test/resources/echofish-dev-output/level_2/Henry_B._Bigelow/HB0707/EK60/HB0707_2.zarr");
//        ZarrArray timeCopy = copy.createArray(
//            "time",
//            new ArrayParams().shape(10001).chunks(1024).dataType(DataType.f8).fillValue(0D)
//        );
//        ZarrArray latitudeCopy = copy.createArray(
//            "latitude",
//            new ArrayParams().shape(10001).chunks(1024).dataType(DataType.f4).fillValue(0F)
//        );
//        ZarrArray longitudeCopy = copy.createArray(
//            "longitude",
//            new ArrayParams().shape(10001).chunks(1024).dataType(DataType.f4).fillValue(0F)
//        );
//
//        timeCopy.write(time.read(new int[]{100001}), new int[]{100001}, new int[] {0});
//        latitudeCopy.write(latitude.read(new int[]{100001}), new int[]{100001}, new int[] {0});
//        longitudeCopy.write(longitude.read(new int[]{100001}), new int[]{100001}, new int[] {0});
//
////        FileUtils.copyDirectory(
////            BUCKET_BASE.resolve(ZARR_BUCKET).resolve("level_2/Henry_B._Bigelow/HB0707/EK60/HB0707.zarr").toFile(),
////            new File("src/test/resources/echofish-dev-output/level_2/Henry_B._Bigelow/HB0707/EK60/HB0707_2.zarr")
////        );
//    }

    @Test
    public void testCreateMvt() throws Exception {

//        List<FileInfoRecord> records = parseCsv(Paths.get("src/test/resources/results.csv")).stream()
//                .sorted(Comparator.comparing(FileInfoRecord::getFileName)).collect(Collectors.toList());
//
//        records.forEach(record -> mapper.save(record, DynamoDBMapperConfig.TableNameOverride.withTableNameReplacement(TABLE_NAME).config()));

        SnsNotifier snsNotifier = mock(SnsNotifier.class);
        when(sns.createNotifier()).thenReturn(snsNotifier);

        CruiseProcessingMessage message = new CruiseProcessingMessage();
        message.setCruiseName("HB0707");
        message.setShipName("Henry_B._Bigelow");
        message.setSensorName("EK60");

        handler.handleRequest(message);


//        verify(snsNotifier).notify(eq(TOPIC_ARN), eq(message));
    }

//    private static CreateTableResult createTable(AmazonDynamoDB ddb, String tableName, String hashKeyName, String rangeKeyName) {
//        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
//        attributeDefinitions.add(new AttributeDefinition(hashKeyName, ScalarAttributeType.S));
//        attributeDefinitions.add(new AttributeDefinition(rangeKeyName, ScalarAttributeType.S));
//
//        List<KeySchemaElement> ks = new ArrayList<>();
//        ks.add(new KeySchemaElement(hashKeyName, KeyType.HASH));
//        ks.add(new KeySchemaElement(rangeKeyName, KeyType.RANGE));
//
//        ProvisionedThroughput provisionedthroughput = new ProvisionedThroughput(1000L, 1000L);
//
//        CreateTableRequest request =
//                new CreateTableRequest()
//                        .withTableName(tableName)
//                        .withAttributeDefinitions(attributeDefinitions)
//                        .withKeySchema(ks)
//                        .withProvisionedThroughput(provisionedthroughput);
//
//        return ddb.createTable(request);
//    }

}