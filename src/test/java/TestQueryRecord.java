//import com.fasterxml.jackson.databind.ObjectMapper;
//import org.apache.avro.file.DataFileReader;
//import org.apache.avro.file.SeekableByteArrayInput;
//import org.apache.avro.generic.GenericDatumReader;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.avro.io.DatumReader;
//import org.apache.nifi.avro.AvroRecordSetWriter;
//import org.apache.nifi.csv.CSVReader;
//import org.apache.nifi.csv.CSVUtils;
//import org.apache.nifi.processors.standard.QueryRecord;
//import org.apache.nifi.reporting.InitializationException;
//import org.apache.nifi.schema.access.SchemaAccessUtils;
//import org.apache.nifi.util.MockFlowFile;
//import org.apache.nifi.util.MockProcessContext;
//import org.apache.nifi.util.MockProcessorInitializationContext;
//import org.apache.nifi.util.TestRunner;
//import org.apache.nifi.util.TestRunners;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.io.IOException;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.util.List;
//import java.util.TreeMap;
//
//public class TestQueryRecord {
//
//    static {
//        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
//        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
//        System.setProperty("org.slf4j.simpleLogger.log.org.apache.calcite", "trace");
//    }
//
//    private TestRunner runner;
//    private QueryRecord processor;
//    private CSVReader reader;
//    private AvroRecordSetWriter writer;
//
//    private String avroSchema = "{\n" +
//            "   \"type\" : \"record\",\n" +
//            "   \"namespace\" : \"nifi\",\n" +
//            "   \"name\" : \"nifi\",\n" +
//            "   \"fields\" : [\n" +
//            "      { \"name\" : \"antiNucleus\" , \"type\" : \"string\" },\n" +
//            "      { \"name\" : \"eventFile\" , \"type\" : \"string\" },\n" +
//            "      { \"name\" : \"eventNumber\" , \"type\" : \"string\" },\n" +
//            "      { \"name\" : \"eventTime\" , \"type\" : \"string\" },\n" +
//            "      { \"name\" : \"histFile\" , \"type\" : \"string\" },\n" +
//            "      { \"name\" : \"multiplicity\" , \"type\" : \"string\" },\n" +
//            "      { \"name\" : \"NaboveLb\" , \"type\" : \"string\" },\n" +
//            "      { \"name\" : \"NbelowLb\" , \"type\" : \"string\" },\n" +
//            "      { \"name\" : \"NLb\" , \"type\" : \"string\" },\n" +
//            "      { \"name\" : \"primaryTracks\" , \"type\" : \"string\" },\n" +
//            "      { \"name\" : \"prodTime\" , \"type\" : \"string\" },\n" +
//            "      { \"name\" : \"Pt\" , \"type\" : \"string\" },\n" +
//            "      { \"name\" : \"runNumber\" , \"type\" : \"string\" },\n" +
//            "      { \"name\" : \"vertexX\" , \"type\" : \"string\" },\n" +
//            "      { \"name\" : \"vertexY\" , \"type\" : \"string\" },\n" +
//            "      { \"name\" : \"vertexZ\" , \"type\" : \"string\" }\n" +
//            "   ]\n" +
//            "}";
//
//    private String sqlQuery = "select  *, row_number() over(partition by 1) id from flowfile";
//
//
//    @Before
//    public void setup() throws InitializationException {
//        processor = new QueryRecord();
//        reader = new CSVReader();
//        writer = new AvroRecordSetWriter();
//        MockProcessContext context = new MockProcessContext(processor);
//        MockProcessorInitializationContext initContext = new MockProcessorInitializationContext(processor, context);
//        processor.initialize(initContext);
//        runner = TestRunners.newTestRunner(processor);
//        runner.setValidateExpressionUsage(false);
//        runner.addControllerService("reader", reader);
////        runner.setProperty(reader, CSVReader.CSV_PARSER, CSVReader.JACKSON_CSV);
////        runner.setProperty(reader, CSVUtils.CSV_FORMAT, CSVUtils.CUSTOM);
////        runner.setProperty(reader, CSVUtils.VALUE_SEPARATOR, ",");
////        runner.setProperty(reader, CSVUtils.QUOTE_CHAR, "\"");
////        runner.setProperty(reader, CSVUtils.ESCAPE_CHAR, "\\");
////        runner.setProperty(reader, CSVUtils.RECORD_SEPARATOR, "\n");
//        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
//        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_TEXT, avroSchema);
//        runner.enableControllerService(reader);
//        runner.addControllerService("writer", writer);
//        runner.setProperty(writer,"compression-format","DEFLATE");
//        runner.enableControllerService(writer);
//        runner.setProperty("record-reader", "reader");
//        runner.setProperty("record-writer", "writer");
//    }
//
//    @Test
//    public void testProcessor() throws IOException {
//        runner.setProperty("query", sqlQuery);
//        Path path = Paths.get("src/test/resources/samples/big_file.aa");
//        runner.enqueue(path);
//        runner.run();
//    }
//
//    @After
//    public void printResults() throws IOException {
//        System.out.println("Printing Results");
//        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("query");
//
//        for (MockFlowFile flowFile : result) {
//            System.out.println(flowFile.toString());
//            System.out.println(flowFile.getSize());
//
////            new TreeMap<>(flowFile.getAttributes()).forEach((k, v) -> {
////                System.out.println(k + ":" + v);
////            });
//            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
//            try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(
//                    new SeekableByteArrayInput(flowFile.toByteArray()), datumReader)) {
////                System.out.println(dataFileReader.getSchema().toString(true));
////                System.out.println(new String(dataFileReader.getMeta("avro.codec")));
//                int recordCount = 0;
//                for (GenericRecord r : dataFileReader) {
//                    if(recordCount == 1) {
//                    ObjectMapper mapper = new ObjectMapper();
//                    Object jsonObject = mapper.readValue(r.toString(), Object.class);
//                    String prettyJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonObject);
//                    System.out.println(prettyJson);
//                    }
//                    recordCount++;
//                }
//                System.out.println(recordCount + " Record(s)");
//            }
//        }
//    }
//}
