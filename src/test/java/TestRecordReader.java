
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.nifi.avro.AvroRecordSetWriter;
import org.apache.nifi.processors.standard.ConvertRecord;
import org.apache.nifi.record.script.ScriptedReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.script.ScriptingComponentUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockProcessorInitializationContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.nifi.util.LogMessage;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestRecordReader {

    private ConvertRecord processor;
    private ScriptedReader reader;
    private TestRunner runner;
    private AvroRecordSetWriter writer;

    @Before
    public void setup() throws InitializationException {
        processor = new ConvertRecord();
        reader = new ScriptedReader();
        writer = new AvroRecordSetWriter();
        MockProcessContext context = new MockProcessContext(processor);
        MockProcessorInitializationContext initContext = new MockProcessorInitializationContext(processor, context);
        processor.initialize(initContext);
        runner = TestRunners.newTestRunner(processor);
        runner.setValidateExpressionUsage(false);
        runner.addControllerService("reader", reader);
        runner.setProperty(reader, "Script Engine", "Groovy");
        runner.setProperty(reader, ScriptingComponentUtils.SCRIPT_FILE, "src/test/resources/scripted_record_reader.groovy");
        runner.enableControllerService(reader);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);
        runner.setProperty("record-reader", "reader");
        runner.setProperty("record-writer", "writer");
    }

    @After
    public void printResults() throws Exception {
        System.out.println("Printing Results");
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
        System.out.println(prettyPrint(result));
    }

    @Test
    public void testProcessor_multiDelim_noHeader_useSchema() throws Exception {
        Path path = Paths.get("src/test/resources/samples/multiDelimiter_noHeader_useSchema.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_schema", "column_1,column_2,column_3");
        attributes.put("wh_txt_delim", "<=>");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
//        System.out.println(prettyPrint(result));
        assertEquals(1, result.size());
        final Schema schema = getSchema(result.get(0));
        final List<Schema.Field> fields = schema.getFields();
        assertEquals(6, fields.size());
        assertEquals("column_1", fields.get(0).name());
        assertEquals("column_2", fields.get(1).name());
        assertEquals("column_3", fields.get(2).name());
        assertEquals("wh_file_date", fields.get(3).name());
        assertEquals("wh_file_id", fields.get(4).name());
        assertEquals("wh_row_id", fields.get(5).name());

        final List<JsonNode> jsonList = getJson(result);
        assertEquals(3, jsonList.size());

        JsonNode record = jsonList.get(0);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("hello", record.get("column_1").asText());
        assertEquals("world", record.get("column_2").asText());
        assertEquals("now", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("1", record.get("wh_row_id").asText());

        record = jsonList.get(1);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("foo", record.get("column_1").asText());
        assertEquals("bar", record.get("column_2").asText());
        assertEquals("fizz", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("2", record.get("wh_row_id").asText());

        record = jsonList.get(2);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("fizz", record.get("column_1").asText());
        assertEquals("buzz", record.get("column_2").asText());
        assertEquals("bar", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("3", record.get("wh_row_id").asText());
    }

    @Test
    public void testProcessor_multiDelim_noHeader_useSchema_extraColsInitial() throws Exception {
        Path path = Paths.get("src/test/resources/samples/multiDelimiter_noHeader_useSchema_extraCols.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_schema", "column_1,column_2,column_3,column_4");
        attributes.put("wh_txt_delim", "<=>");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Expected 4 fields but encountered 3 on row 2"));
    }

    @Test
    public void testProcessor_multiDelim_noHeader_useSchema_extraColsLater() throws Exception {
        Path path = Paths.get("src/test/resources/samples/multiDelimiter_noHeader_useSchema_extraColsLater.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_schema", "column_1,column_2,column_3");
        attributes.put("wh_txt_delim", "<=>");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Expected 3 fields but encountered 4 on row 2"));
    }

    @Test
    public void testProcessor_multiDelim_noHeader_useSchema_schemaHasLessColumns() throws Exception {
        Path path = Paths.get("src/test/resources/samples/multiDelimiter_noHeader_useSchema.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_delim", "<=>");
        attributes.put("wh_txt_schema", "column_1,column_2");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("File has more columns than schema."));
    }

    @Test
    public void testProcessor_multiDelim_noHeader_useSchema_schemaHasMoreColumns() throws Exception {
        Path path = Paths.get("src/test/resources/samples/multiDelimiter_noHeader_useSchema.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_schema", "column_1,column_2,column_3,column_4");
        attributes.put("wh_txt_delim", "<=>");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
//        System.out.println(prettyPrint(result));
        assertEquals(1, result.size());
        final Schema schema = getSchema(result.get(0));
        final List<Schema.Field> fields = schema.getFields();
        assertEquals(7, fields.size());
        assertEquals("column_1", fields.get(0).name());
        assertEquals("column_2", fields.get(1).name());
        assertEquals("column_3", fields.get(2).name());
        assertEquals("column_4", fields.get(3).name());
        assertEquals("wh_file_date", fields.get(4).name());
        assertEquals("wh_file_id", fields.get(5).name());
        assertEquals("wh_row_id", fields.get(6).name());

        final List<JsonNode> jsonList = getJson(result);
        assertEquals(3, jsonList.size());

        JsonNode record = jsonList.get(0);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("column_4"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("hello", record.get("column_1").asText());
        assertEquals("world", record.get("column_2").asText());
        assertEquals("now", record.get("column_3").asText());
        assertEquals("null", record.get("column_4").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("1", record.get("wh_row_id").asText());

        record = jsonList.get(1);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("foo", record.get("column_1").asText());
        assertEquals("bar", record.get("column_2").asText());
        assertEquals("fizz", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("2", record.get("wh_row_id").asText());

        record = jsonList.get(2);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("fizz", record.get("column_1").asText());
        assertEquals("buzz", record.get("column_2").asText());
        assertEquals("bar", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("3", record.get("wh_row_id").asText());
    }

    @Test
    public void testProcessor_multiDelim_noHeader_useSchema_skipLines() throws Exception {
        Path path = Paths.get("src/test/resources/samples/multiDelimiter_noHeader_useSchema_skipLines.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_schema", "column_1,column_2,column_3");
        attributes.put("wh_txt_delim", "<=>");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
//        System.out.println(prettyPrint(result));
        assertEquals(1, result.size());
        final Schema schema = getSchema(result.get(0));
        final List<Schema.Field> fields = schema.getFields();
        assertEquals(6, fields.size());
        assertEquals("column_1", fields.get(0).name());
        assertEquals("column_2", fields.get(1).name());
        assertEquals("column_3", fields.get(2).name());
        assertEquals("wh_file_date", fields.get(3).name());
        assertEquals("wh_file_id", fields.get(4).name());
        assertEquals("wh_row_id", fields.get(5).name());

        final List<JsonNode> jsonList = getJson(result);
        assertEquals(3, jsonList.size());

        JsonNode record = jsonList.get(0);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("hello", record.get("column_1").asText());
        assertEquals("world", record.get("column_2").asText());
        assertEquals("now", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("1", record.get("wh_row_id").asText());

        record = jsonList.get(1);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("foo", record.get("column_1").asText());
        assertEquals("bar", record.get("column_2").asText());
        assertEquals("fizz", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("2", record.get("wh_row_id").asText());

        record = jsonList.get(2);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("fizz", record.get("column_1").asText());
        assertEquals("buzz", record.get("column_2").asText());
        assertEquals("bar", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("3", record.get("wh_row_id").asText());
    }

    @Test
    public void testProcessor_multiDelim_noHeader_useSchema_skipLines_extraColsInitial() throws Exception {
        Path path = Paths.get("src/test/resources/samples/multiDelimiter_noHeader_useSchema_skipLines_extraCols.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_schema", "column_1,column_2,column_3,column_4");
        attributes.put("wh_txt_delim", "<=>");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Expected 4 fields but encountered 3 on row 2"));
    }

    @Test
    public void testProcessor_multiDelim_noHeader_useSchema_skipLines_extraColsLater() throws Exception {
        Path path = Paths.get("src/test/resources/samples/multiDelimiter_noHeader_useSchema_skipLines_extraColsLater.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_schema", "column_1,column_2,column_3");
        attributes.put("wh_txt_delim", "<=>");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Expected 3 fields but encountered 4 on row 2"));
    }

    @Test
    public void testProcessor_multiDelim_noHeader_useSchema_skipLines_schemaHasLessColumns() throws Exception {
        Path path = Paths.get("src/test/resources/samples/multiDelimiter_noHeader_useSchema_skipLines.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_delim", "<=>");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_txt_schema", "column_1,column_2");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("File has more columns than schema."));
    }

    @Test
    public void testProcessor_multiDelim_noHeader_useSchema_skipLines_schemaHasMoreColumns() throws Exception {
        Path path = Paths.get("src/test/resources/samples/multiDelimiter_noHeader_useSchema_skipLines.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_schema", "column_1,column_2,column_3,column_4");
        attributes.put("wh_txt_delim", "<=>");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
//        System.out.println(prettyPrint(result));

        assertEquals(1, result.size());
        final Schema schema = getSchema(result.get(0));
        final List<Schema.Field> fields = schema.getFields();
        assertEquals(7, fields.size());
        assertEquals("column_1", fields.get(0).name());
        assertEquals("column_2", fields.get(1).name());
        assertEquals("column_3", fields.get(2).name());
        assertEquals("column_4", fields.get(3).name());
        assertEquals("wh_file_date", fields.get(4).name());
        assertEquals("wh_file_id", fields.get(5).name());
        assertEquals("wh_row_id", fields.get(6).name());

        final List<JsonNode> jsonList = getJson(result);
        assertEquals(3, jsonList.size());

        JsonNode record = jsonList.get(0);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("column_4"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("hello", record.get("column_1").asText());
        assertEquals("world", record.get("column_2").asText());
        assertEquals("now", record.get("column_3").asText());
        assertEquals("null", record.get("column_4").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("1", record.get("wh_row_id").asText());

        record = jsonList.get(1);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("foo", record.get("column_1").asText());
        assertEquals("bar", record.get("column_2").asText());
        assertEquals("fizz", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("2", record.get("wh_row_id").asText());

        record = jsonList.get(2);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("fizz", record.get("column_1").asText());
        assertEquals("buzz", record.get("column_2").asText());
        assertEquals("bar", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("3", record.get("wh_row_id").asText());
    }

    @Test
    public void testProcessor_multiDelim_useHeader_noSchema() throws Exception {
        Path path = Paths.get("src/test/resources/samples/multiDelimiter_useHeader_noSchema.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "<=>");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
//        System.out.println(prettyPrint(result));

        assertEquals(1, result.size());
        final Schema schema = getSchema(result.get(0));
        final List<Schema.Field> fields = schema.getFields();
        assertEquals(6, fields.size());
        assertEquals("column_1", fields.get(0).name());
        assertEquals("column_2", fields.get(1).name());
        assertEquals("column_3", fields.get(2).name());
        assertEquals("wh_file_date", fields.get(3).name());
        assertEquals("wh_file_id", fields.get(4).name());
        assertEquals("wh_row_id", fields.get(5).name());

        final List<JsonNode> jsonList = getJson(result);
        assertEquals(3, jsonList.size());

        JsonNode record = jsonList.get(0);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("hello", record.get("column_1").asText());
        assertEquals("world", record.get("column_2").asText());
        assertEquals("now", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("1", record.get("wh_row_id").asText());

        record = jsonList.get(1);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("foo", record.get("column_1").asText());
        assertEquals("bar", record.get("column_2").asText());
        assertEquals("fizz", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("2", record.get("wh_row_id").asText());

        record = jsonList.get(2);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("fizz", record.get("column_1").asText());
        assertEquals("buzz", record.get("column_2").asText());
        assertEquals("bar", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("3", record.get("wh_row_id").asText());
    }

    @Test
    public void testProcessor_multiDelim_useHeader_noSchema_extraColsInitial() throws Exception {
        Path path = Paths.get("src/test/resources/samples/multiDelimiter_useHeader_noSchema_extraCols.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "<=>");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Expected 4 fields but encountered 3 on row 1"));
    }

    @Test
    public void testProcessor_multiDelim_useHeader_noSchema_extraColsLater() throws Exception {
        Path path = Paths.get("src/test/resources/samples/multiDelimiter_useHeader_noSchema_extraColsLater.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "<=>");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Expected 3 fields but encountered 4 on row 2"));
    }

    @Test
    public void testProcessor_multiDelim_useHeader_noSchema_skipLines() throws Exception {
        Path path = Paths.get("src/test/resources/samples/multiDelimiter_useHeader_noSchema_skipLines.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "<=>");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
//        System.out.println(prettyPrint(result));

        assertEquals(1, result.size());
        final Schema schema = getSchema(result.get(0));
        final List<Schema.Field> fields = schema.getFields();
        assertEquals(6, fields.size());
        assertEquals("column_1", fields.get(0).name());
        assertEquals("column_2", fields.get(1).name());
        assertEquals("column_3", fields.get(2).name());
        assertEquals("wh_file_date", fields.get(3).name());
        assertEquals("wh_file_id", fields.get(4).name());
        assertEquals("wh_row_id", fields.get(5).name());

        final List<JsonNode> jsonList = getJson(result);
        assertEquals(3, jsonList.size());

        JsonNode record = jsonList.get(0);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("hello", record.get("column_1").asText());
        assertEquals("world", record.get("column_2").asText());
        assertEquals("now", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("1", record.get("wh_row_id").asText());

        record = jsonList.get(1);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("foo", record.get("column_1").asText());
        assertEquals("bar", record.get("column_2").asText());
        assertEquals("fizz", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("2", record.get("wh_row_id").asText());

        record = jsonList.get(2);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("fizz", record.get("column_1").asText());
        assertEquals("buzz", record.get("column_2").asText());
        assertEquals("bar", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("3", record.get("wh_row_id").asText());
    }

    @Test
    public void testProcessor_multiDelim_useHeader_noSchema_skipLines_extraColsInitial() throws Exception {
        Path path = Paths.get("src/test/resources/samples/multiDelimiter_useHeader_noSchema_skipLines_extraCols.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "<=>");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Expected 4 fields but encountered 3 on row 1"));
    }

    @Test
    public void testProcessor_multiDelim_useHeader_noSchema_skipLines_extraColsLater() throws Exception {
        Path path = Paths.get("src/test/resources/samples/multiDelimiter_useHeader_noSchema_skipLines_extraColsLater.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "<=>");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Expected 3 fields but encountered 4 on row 2"));
    }

    @Test
    public void testProcessor_multiDelim_useHeader_useSchema() throws Exception {
        Path path = Paths.get("src/test/resources/samples/multiDelimiter_useHeader_useSchema.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "<=>");
        attributes.put("wh_txt_schema", "column_1,column_2,column_3");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
//        System.out.println(prettyPrint(result));

        assertEquals(1, result.size());
        final Schema schema = getSchema(result.get(0));
        final List<Schema.Field> fields = schema.getFields();
        assertEquals(6, fields.size());
        assertEquals("column_1", fields.get(0).name());
        assertEquals("column_2", fields.get(1).name());
        assertEquals("column_3", fields.get(2).name());
        assertEquals("wh_file_date", fields.get(3).name());
        assertEquals("wh_file_id", fields.get(4).name());
        assertEquals("wh_row_id", fields.get(5).name());

        final List<JsonNode> jsonList = getJson(result);
        assertEquals(3, jsonList.size());

        JsonNode record = jsonList.get(0);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("hello", record.get("column_1").asText());
        assertEquals("world", record.get("column_2").asText());
        assertEquals("now", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("1", record.get("wh_row_id").asText());

        record = jsonList.get(1);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("foo", record.get("column_1").asText());
        assertEquals("bar", record.get("column_2").asText());
        assertEquals("fizz", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("2", record.get("wh_row_id").asText());

        record = jsonList.get(2);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("fizz", record.get("column_1").asText());
        assertEquals("buzz", record.get("column_2").asText());
        assertEquals("bar", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("3", record.get("wh_row_id").asText());
    }

    @Test
    public void testProcessor_multiDelim_useHeader_useSchema_extraColsInitial() throws Exception {
        Path path = Paths.get("src/test/resources/samples/multiDelimiter_useHeader_useSchema_extraCols.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_schema", "column_1,column_2,column_3,column_4");
        attributes.put("wh_txt_delim", "<=>");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Expected 4 fields but encountered 3 on row 1"));
    }

    @Test
    public void testProcessor_multiDelim_useHeader_useSchema_extraColsLater() throws Exception {
        Path path = Paths.get("src/test/resources/samples/multiDelimiter_useHeader_useSchema_extraColsLater.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_schema", "column_1,column_2,column_3");
        attributes.put("wh_txt_delim", "<=>");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Expected 3 fields but encountered 4 on row 2"));
    }

    @Test
    public void testProcessor_multiDelim_useHeader_useSchema_headerHasDiffCol() throws Exception {
        Path path = Paths.get("src/test/resources/samples/multiDelimiter_useHeader_useSchema_diffCol.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "<=>");
        attributes.put("wh_txt_schema", "column_1,column_2,column_3");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Column 'column_4' not in schema."));
    }

    @Test
    public void testProcessor_multiDelim_useHeader_useSchema_schemaHasLessColumns() throws Exception {
        Path path = Paths.get("src/test/resources/samples/multiDelimiter_useHeader_useSchema.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "<=>");
        attributes.put("wh_txt_schema", "column_1,column_2");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("File has more columns than schema."));
    }

    @Test
    public void testProcessor_multiDelim_useHeader_useSchema_schemaHasMoreColumns() throws Exception {
        Path path = Paths.get("src/test/resources/samples/multiDelimiter_useHeader_useSchema.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "<=>");
        attributes.put("wh_txt_schema", "column_1,column_2,column_3,column_4");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
//        System.out.println(prettyPrint(result));

        assertEquals(1, result.size());
        final Schema schema = getSchema(result.get(0));
        final List<Schema.Field> fields = schema.getFields();
        assertEquals(7, fields.size());

        assertEquals("column_1", fields.get(0).name());
        assertEquals("column_2", fields.get(1).name());
        assertEquals("column_3", fields.get(2).name());
        assertEquals("column_4", fields.get(3).name());
        assertEquals("wh_file_date", fields.get(4).name());
        assertEquals("wh_file_id", fields.get(5).name());
        assertEquals("wh_row_id", fields.get(6).name());

        final List<JsonNode> jsonList = getJson(result);
        assertEquals(3, jsonList.size());

        JsonNode record = jsonList.get(0);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("column_4"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("hello", record.get("column_1").asText());
        assertEquals("world", record.get("column_2").asText());
        assertEquals("now", record.get("column_3").asText());
        assertEquals("null", record.get("column_4").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("1", record.get("wh_row_id").asText());

        record = jsonList.get(1);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("column_4"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("foo", record.get("column_1").asText());
        assertEquals("bar", record.get("column_2").asText());
        assertEquals("fizz", record.get("column_3").asText());
        assertEquals("null", record.get("column_4").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("2", record.get("wh_row_id").asText());

        record = jsonList.get(2);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("column_4"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("fizz", record.get("column_1").asText());
        assertEquals("buzz", record.get("column_2").asText());
        assertEquals("bar", record.get("column_3").asText());
        assertEquals("null", record.get("column_4").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("3", record.get("wh_row_id").asText());
    }

    @Test
    public void testProcessor_multiDelim_useHeader_useSchema_skipLines() throws Exception {
        Path path = Paths.get("src/test/resources/samples/multiDelimiter_useHeader_useSchema_skipLines.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "<=>");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_txt_schema", "column_1,column_2,column_3");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
//        System.out.println(prettyPrint(result));

        assertEquals(1, result.size());
        final Schema schema = getSchema(result.get(0));
        final List<Schema.Field> fields = schema.getFields();
        assertEquals(6, fields.size());
        assertEquals("column_1", fields.get(0).name());
        assertEquals("column_2", fields.get(1).name());
        assertEquals("column_3", fields.get(2).name());
        assertEquals("wh_file_date", fields.get(3).name());
        assertEquals("wh_file_id", fields.get(4).name());
        assertEquals("wh_row_id", fields.get(5).name());

        final List<JsonNode> jsonList = getJson(result);
        assertEquals(3, jsonList.size());

        JsonNode record = jsonList.get(0);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("hello", record.get("column_1").asText());
        assertEquals("world", record.get("column_2").asText());
        assertEquals("now", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("1", record.get("wh_row_id").asText());

        record = jsonList.get(1);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("foo", record.get("column_1").asText());
        assertEquals("bar", record.get("column_2").asText());
        assertEquals("fizz", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("2", record.get("wh_row_id").asText());

        record = jsonList.get(2);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("fizz", record.get("column_1").asText());
        assertEquals("buzz", record.get("column_2").asText());
        assertEquals("bar", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("3", record.get("wh_row_id").asText());
    }

    @Test
    public void testProcessor_multiDelim_useHeader_useSchema_skipLines_extraColsInitial() throws Exception {
        Path path = Paths.get("src/test/resources/samples/multiDelimiter_useHeader_useSchema_skipLines_extraCols.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_schema", "column_1,column_2,column_3,column_4");
        attributes.put("wh_txt_delim", "<=>");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Expected 4 fields but encountered 3 on row 1"));
    }

    @Test
    public void testProcessor_multiDelim_useHeader_useSchema_skipLines_extraColsLater() throws Exception {
        Path path = Paths.get("src/test/resources/samples/multiDelimiter_useHeader_useSchema_skipLines_extraColsLater.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_schema", "column_1,column_2,column_3");
        attributes.put("wh_txt_delim", "<=>");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Expected 3 fields but encountered 4 on row 2"));
    }

    @Test
    public void testProcessor_multiDelim_useHeader_useSchema_skipLines_headerHasDiffCol() throws Exception {
        Path path = Paths.get("src/test/resources/samples/multiDelimiter_useHeader_useSchema_skipLines_diffCol.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "<=>");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_txt_schema", "column_1,column_2,column_3");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Column 'column_4' not in schema."));
    }

    @Test
    public void testProcessor_multiDelim_useHeader_useSchema_skipLines_schemaHasLessColumns() throws Exception {
        Path path = Paths.get("src/test/resources/samples/multiDelimiter_useHeader_useSchema_skipLines.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "<=>");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_txt_schema", "column_1,column_2");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("File has more columns than schema."));
    }

    @Test
    public void testProcessor_multiDelim_useHeader_useSchema_skipLines_schemaHasMoreColumns() throws Exception {
        Path path = Paths.get("src/test/resources/samples/multiDelimiter_useHeader_useSchema_skipLines.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "<=>");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_txt_schema", "column_1,column_2,column_3,column_4");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
//        System.out.println(prettyPrint(result));

        assertEquals(1, result.size());
        final Schema schema = getSchema(result.get(0));
        final List<Schema.Field> fields = schema.getFields();
        assertEquals(7, fields.size());

        assertEquals("column_1", fields.get(0).name());
        assertEquals("column_2", fields.get(1).name());
        assertEquals("column_3", fields.get(2).name());
        assertEquals("column_4", fields.get(3).name());
        assertEquals("wh_file_date", fields.get(4).name());
        assertEquals("wh_file_id", fields.get(5).name());
        assertEquals("wh_row_id", fields.get(6).name());

        final List<JsonNode> jsonList = getJson(result);
        assertEquals(3, jsonList.size());

        JsonNode record = jsonList.get(0);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("column_4"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("hello", record.get("column_1").asText());
        assertEquals("world", record.get("column_2").asText());
        assertEquals("now", record.get("column_3").asText());
        assertEquals("null", record.get("column_4").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("1", record.get("wh_row_id").asText());

        record = jsonList.get(1);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("column_4"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("foo", record.get("column_1").asText());
        assertEquals("bar", record.get("column_2").asText());
        assertEquals("fizz", record.get("column_3").asText());
        assertEquals("null", record.get("column_4").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("2", record.get("wh_row_id").asText());

        record = jsonList.get(2);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("column_4"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("fizz", record.get("column_1").asText());
        assertEquals("buzz", record.get("column_2").asText());
        assertEquals("bar", record.get("column_3").asText());
        assertEquals("null", record.get("column_4").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("3", record.get("wh_row_id").asText());
    }

    @Test
    public void testProcessor_noDelimiter() throws Exception {
        Path path = Paths.get("src/test/resources/samples/noHeader_useSchema.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
//        System.out.println(prettyPrint(result));

        assertEquals(1, result.size());
        final Schema schema = getSchema(result.get(0));
        final List<Schema.Field> fields = schema.getFields();
        assertEquals(4, fields.size());
        assertEquals("row_data", fields.get(0).name());
        assertEquals("wh_file_date", fields.get(1).name());
        assertEquals("wh_file_id", fields.get(2).name());
        assertEquals("wh_row_id", fields.get(3).name());

        final List<JsonNode> jsonList = getJson(result);
        assertEquals(3, jsonList.size());

        JsonNode record = jsonList.get(0);

        assertTrue(record.has("row_data"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("hello|world|now", record.get("row_data").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("1", record.get("wh_row_id").asText());

        record = jsonList.get(1);

        assertTrue(record.has("row_data"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("foo|bar|fizz", record.get("row_data").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("2", record.get("wh_row_id").asText());

        record = jsonList.get(2);

        assertTrue(record.has("row_data"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("fizz|buzz|bar", record.get("row_data").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("3", record.get("wh_row_id").asText());
    }

    @Test
    public void testProcessor_noHeader_noSchema() throws Exception {
        Path path = Paths.get("src/test/resources/samples/noHeader_useSchema.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Useheader or schema must be provided."));
    }

    @Test
    public void testProcessor_noHeader_useSchema() throws Exception {
        Path path = Paths.get("src/test/resources/samples/noHeader_useSchema.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_schema", "column_1,column_2,column_3");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
//        System.out.println(prettyPrint(result));

        assertEquals(1, result.size());
        final Schema schema = getSchema(result.get(0));
        final List<Schema.Field> fields = schema.getFields();
        assertEquals(6, fields.size());
        assertEquals("column_1", fields.get(0).name());
        assertEquals("column_2", fields.get(1).name());
        assertEquals("column_3", fields.get(2).name());
        assertEquals("wh_file_date", fields.get(3).name());
        assertEquals("wh_file_id", fields.get(4).name());
        assertEquals("wh_row_id", fields.get(5).name());

        final List<JsonNode> jsonList = getJson(result);
        assertEquals(3, jsonList.size());

        JsonNode record = jsonList.get(0);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("hello", record.get("column_1").asText());
        assertEquals("world", record.get("column_2").asText());
        assertEquals("now", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("1", record.get("wh_row_id").asText());

        record = jsonList.get(1);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("foo", record.get("column_1").asText());
        assertEquals("bar", record.get("column_2").asText());
        assertEquals("fizz", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("2", record.get("wh_row_id").asText());

        record = jsonList.get(2);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("fizz", record.get("column_1").asText());
        assertEquals("buzz", record.get("column_2").asText());
        assertEquals("bar", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("3", record.get("wh_row_id").asText());
    }

    @Test
    public void testProcessor_noHeader_useSchema_schemaHasLessColumns() throws Exception {
        Path path = Paths.get("src/test/resources/samples/noHeader_useSchema.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_txt_schema", "column_1,column_2");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("File has more columns than schema."));
    }

    @Test
    public void testProcessor_noHeader_useSchema_schemaHasMoreColumns() throws Exception {
        Path path = Paths.get("src/test/resources/samples/noHeader_useSchema.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_schema", "column_1,column_2,column_3,column_4");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
//        System.out.println(prettyPrint(result));

        assertEquals(1, result.size());
        final Schema schema = getSchema(result.get(0));
        final List<Schema.Field> fields = schema.getFields();
        assertEquals(7, fields.size());
        assertEquals("column_1", fields.get(0).name());
        assertEquals("column_2", fields.get(1).name());
        assertEquals("column_3", fields.get(2).name());
        assertEquals("column_4", fields.get(3).name());
        assertEquals("wh_file_date", fields.get(4).name());
        assertEquals("wh_file_id", fields.get(5).name());
        assertEquals("wh_row_id", fields.get(6).name());

        final List<JsonNode> jsonList = getJson(result);
        assertEquals(3, jsonList.size());

        JsonNode record = jsonList.get(0);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("column_4"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("hello", record.get("column_1").asText());
        assertEquals("world", record.get("column_2").asText());
        assertEquals("now", record.get("column_3").asText());
        assertEquals("null", record.get("column_4").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("1", record.get("wh_row_id").asText());

        record = jsonList.get(1);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("foo", record.get("column_1").asText());
        assertEquals("bar", record.get("column_2").asText());
        assertEquals("fizz", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("2", record.get("wh_row_id").asText());

        record = jsonList.get(2);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("fizz", record.get("column_1").asText());
        assertEquals("buzz", record.get("column_2").asText());
        assertEquals("bar", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("3", record.get("wh_row_id").asText());
    }

    @Test
    public void testProcessor_noHeader_useSchema_skipLines() throws Exception {
        Path path = Paths.get("src/test/resources/samples/noHeader_useSchema_skipLines.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_schema", "column_1,column_2,column_3");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
//        System.out.println(prettyPrint(result));

        assertEquals(1, result.size());
        final Schema schema = getSchema(result.get(0));
        final List<Schema.Field> fields = schema.getFields();
        assertEquals(6, fields.size());
        assertEquals("column_1", fields.get(0).name());
        assertEquals("column_2", fields.get(1).name());
        assertEquals("column_3", fields.get(2).name());
        assertEquals("wh_file_date", fields.get(3).name());
        assertEquals("wh_file_id", fields.get(4).name());
        assertEquals("wh_row_id", fields.get(5).name());

        final List<JsonNode> jsonList = getJson(result);
        assertEquals(3, jsonList.size());

        JsonNode record = jsonList.get(0);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("hello", record.get("column_1").asText());
        assertEquals("world", record.get("column_2").asText());
        assertEquals("now", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("1", record.get("wh_row_id").asText());

        record = jsonList.get(1);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("foo", record.get("column_1").asText());
        assertEquals("bar", record.get("column_2").asText());
        assertEquals("fizz", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("2", record.get("wh_row_id").asText());

        record = jsonList.get(2);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("fizz", record.get("column_1").asText());
        assertEquals("buzz", record.get("column_2").asText());
        assertEquals("bar", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("3", record.get("wh_row_id").asText());
    }

    @Test
    public void testProcessor_noHeader_useSchema_skipLines_extraColsInitial() throws Exception {
        Path path = Paths.get("src/test/resources/samples/noHeader_useSchema_skipLines_extraCols.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_schema", "column_1,column_2,column_3,column_4");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Expected 4 fields but encountered 3 on row 2"));
    }

    @Test
    public void testProcessor_noHeader_useSchema_skipLines_extraColsLater() throws Exception {
        Path path = Paths.get("src/test/resources/samples/noHeader_useSchema_skipLines_extraColsLater.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_schema", "column_1,column_2,column_3");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Expected 3 fields but encountered 4 on row 2"));
    }

    @Test
    public void testProcessor_noHeader_useSchema_skipLines_schemaHasLessColumns() throws Exception {
        Path path = Paths.get("src/test/resources/samples/noHeader_useSchema_skipLines.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_txt_schema", "column_1,column_2");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("File has more columns than schema."));
    }

    @Test
    public void testProcessor_noHeader_useSchema_skipLines_schemaHasMoreColumns() throws Exception {
        Path path = Paths.get("src/test/resources/samples/noHeader_useSchema_skipLines.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_schema", "column_1,column_2,column_3,column_4");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
//        System.out.println(prettyPrint(result));

        assertEquals(1, result.size());
        final Schema schema = getSchema(result.get(0));
        final List<Schema.Field> fields = schema.getFields();
        assertEquals(7, fields.size());
        assertEquals("column_1", fields.get(0).name());
        assertEquals("column_2", fields.get(1).name());
        assertEquals("column_3", fields.get(2).name());
        assertEquals("column_4", fields.get(3).name());
        assertEquals("wh_file_date", fields.get(4).name());
        assertEquals("wh_file_id", fields.get(5).name());
        assertEquals("wh_row_id", fields.get(6).name());

        final List<JsonNode> jsonList = getJson(result);
        assertEquals(3, jsonList.size());

        JsonNode record = jsonList.get(0);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("column_4"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("hello", record.get("column_1").asText());
        assertEquals("world", record.get("column_2").asText());
        assertEquals("now", record.get("column_3").asText());
        assertEquals("null", record.get("column_4").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("1", record.get("wh_row_id").asText());

        record = jsonList.get(1);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("foo", record.get("column_1").asText());
        assertEquals("bar", record.get("column_2").asText());
        assertEquals("fizz", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("2", record.get("wh_row_id").asText());

        record = jsonList.get(2);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("fizz", record.get("column_1").asText());
        assertEquals("buzz", record.get("column_2").asText());
        assertEquals("bar", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("3", record.get("wh_row_id").asText());
    }

    @Test
    public void testProcessor_noHeader_useSchema_skipLines_withQuotesandEscape() throws Exception {
        Path path = Paths.get("src/test/resources/samples/noHeader_skipLines_withQuotes_withEscape.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_schema", "column_1,column_2,column_3");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_txt_quote", "\"");
        attributes.put("wh_txt_escape", "\\");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
//        System.out.println(prettyPrint(result));

        assertEquals(1, result.size());
        final Schema schema = getSchema(result.get(0));
        final List<Schema.Field> fields = schema.getFields();
        assertEquals(6, fields.size());
        assertEquals("column_1", fields.get(0).name());
        assertEquals("column_2", fields.get(1).name());
        assertEquals("column_3", fields.get(2).name());
        assertEquals("wh_file_date", fields.get(3).name());
        assertEquals("wh_file_id", fields.get(4).name());
        assertEquals("wh_row_id", fields.get(5).name());

        final List<JsonNode> jsonList = getJson(result);
        assertEquals(3, jsonList.size());

        JsonNode record = jsonList.get(0);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("hello", record.get("column_1").asText());
        assertEquals("wo\"|rld", record.get("column_2").asText());
        assertEquals("now", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("1", record.get("wh_row_id").asText());

        record = jsonList.get(1);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("foo", record.get("column_1").asText());
        assertEquals("bar", record.get("column_2").asText());
        assertEquals("fizz", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("2", record.get("wh_row_id").asText());

        record = jsonList.get(2);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("fizz", record.get("column_1").asText());
        assertEquals("buzz", record.get("column_2").asText());
        assertEquals("bar", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("3", record.get("wh_row_id").asText());
    }

    @Test
    public void testProcessor_noHeader_useSchema_withQuotesandEscape() throws Exception {
        Path path = Paths.get("src/test/resources/samples/noHeader_withQuotes_withEscape.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_schema", "column_1,column_2,column_3");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_txt_quote", "\"");
        attributes.put("wh_txt_escape", "\\");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
//        System.out.println(prettyPrint(result));

        assertEquals(1, result.size());
        final Schema schema = getSchema(result.get(0));
        final List<Schema.Field> fields = schema.getFields();
        assertEquals(6, fields.size());
        assertEquals("column_1", fields.get(0).name());
        assertEquals("column_2", fields.get(1).name());
        assertEquals("column_3", fields.get(2).name());
        assertEquals("wh_file_date", fields.get(3).name());
        assertEquals("wh_file_id", fields.get(4).name());
        assertEquals("wh_row_id", fields.get(5).name());

        final List<JsonNode> jsonList = getJson(result);
        assertEquals(3, jsonList.size());

        JsonNode record = jsonList.get(0);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("hello", record.get("column_1").asText());
        assertEquals("wo\"|rld", record.get("column_2").asText());
        assertEquals("now", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("1", record.get("wh_row_id").asText());

        record = jsonList.get(1);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("foo", record.get("column_1").asText());
        assertEquals("bar", record.get("column_2").asText());
        assertEquals("fizz", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("2", record.get("wh_row_id").asText());

        record = jsonList.get(2);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("fizz", record.get("column_1").asText());
        assertEquals("buzz", record.get("column_2").asText());
        assertEquals("bar", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("3", record.get("wh_row_id").asText());
    }

    @Test
    public void testProcessor_noHeader_useSchema_extraColsInitial() throws Exception {
        Path path = Paths.get("src/test/resources/samples/noHeader_useSchema_extraCols.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_schema", "column_1,column_2,column_3,column_4");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Expected 4 fields but encountered 3 on row 2"));
    }

    @Test
    public void testProcessor_noHeader_useSchema_extraColsLater() throws Exception {
        Path path = Paths.get("src/test/resources/samples/noHeader_useSchema_extraColsLater.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_schema", "column_1,column_2,column_3");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Expected 3 fields but encountered 4 on row 2"));
    }

    @Test
    public void testProcessor_useHeader_noSchema() throws Exception {
        Path path = Paths.get("src/test/resources/samples/useHeader_noSchema.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
//        System.out.println(prettyPrint(result));

        assertEquals(1, result.size());
        final Schema schema = getSchema(result.get(0));
        final List<Schema.Field> fields = schema.getFields();
        assertEquals(6, fields.size());
        assertEquals("column_1", fields.get(0).name());
        assertEquals("column_2", fields.get(1).name());
        assertEquals("column_3", fields.get(2).name());
        assertEquals("wh_file_date", fields.get(3).name());
        assertEquals("wh_file_id", fields.get(4).name());
        assertEquals("wh_row_id", fields.get(5).name());

        final List<JsonNode> jsonList = getJson(result);
        assertEquals(3, jsonList.size());

        JsonNode record = jsonList.get(0);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("hello", record.get("column_1").asText());
        assertEquals("world", record.get("column_2").asText());
        assertEquals("now", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("1", record.get("wh_row_id").asText());

        record = jsonList.get(1);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("foo", record.get("column_1").asText());
        assertEquals("bar", record.get("column_2").asText());
        assertEquals("fizz", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("2", record.get("wh_row_id").asText());

        record = jsonList.get(2);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("fizz", record.get("column_1").asText());
        assertEquals("buzz", record.get("column_2").asText());
        assertEquals("bar", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("3", record.get("wh_row_id").asText());
    }

    @Test
    public void testProcessor_useHeader_noSchema_extraColsInitial() throws Exception {
        Path path = Paths.get("src/test/resources/samples/useHeader_noSchema_extraCols.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Expected 4 fields but encountered 3 on row 1"));
    }

    @Test
    public void testProcessor_useHeader_noSchema_extraColsLater() throws Exception {
        Path path = Paths.get("src/test/resources/samples/useHeader_noSchema_extraColsLater.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Expected 3 fields but encountered 4 on row 2"));
    }

    @Test
    public void testProcessor_useHeader_noSchema_skipLines() throws Exception {
        Path path = Paths.get("src/test/resources/samples/useHeader_noSchema_skipLines.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
//        System.out.println(prettyPrint(result));

        assertEquals(1, result.size());
        final Schema schema = getSchema(result.get(0));
        final List<Schema.Field> fields = schema.getFields();
        assertEquals(6, fields.size());
        assertEquals("column_1", fields.get(0).name());
        assertEquals("column_2", fields.get(1).name());
        assertEquals("column_3", fields.get(2).name());
        assertEquals("wh_file_date", fields.get(3).name());
        assertEquals("wh_file_id", fields.get(4).name());
        assertEquals("wh_row_id", fields.get(5).name());

        final List<JsonNode> jsonList = getJson(result);
        assertEquals(3, jsonList.size());

        JsonNode record = jsonList.get(0);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("hello", record.get("column_1").asText());
        assertEquals("world", record.get("column_2").asText());
        assertEquals("now", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("1", record.get("wh_row_id").asText());

        record = jsonList.get(1);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("foo", record.get("column_1").asText());
        assertEquals("bar", record.get("column_2").asText());
        assertEquals("fizz", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("2", record.get("wh_row_id").asText());

        record = jsonList.get(2);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("fizz", record.get("column_1").asText());
        assertEquals("buzz", record.get("column_2").asText());
        assertEquals("bar", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("3", record.get("wh_row_id").asText());
    }

    @Test
    public void testProcessor_useHeader_noSchema_skipLines_extraColsInitial() throws Exception {
        Path path = Paths.get("src/test/resources/samples/useHeader_noSchema_skipLines_extraCols.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Expected 4 fields but encountered 3 on row 1"));
    }

    @Test
    public void testProcessor_useHeader_noSchema_skipLines_extraColsLater() throws Exception {
        Path path = Paths.get("src/test/resources/samples/useHeader_noSchema_skipLines_extraColsLater.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Expected 3 fields but encountered 4 on row 2"));
    }

    @Test
    public void testProcessor_useHeader_noSchema_skipLines_withQuotesandEscape() throws Exception {
        Path path = Paths.get("src/test/resources/samples/skipLines_withHeader_withQuotes_withEscape.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_txt_quote", "\"");
        attributes.put("wh_txt_escape", "\\");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
//        System.out.println(prettyPrint(result));

        assertEquals(1, result.size());
        final Schema schema = getSchema(result.get(0));
        final List<Schema.Field> fields = schema.getFields();
        assertEquals(6, fields.size());
        assertEquals("column_1", fields.get(0).name());
        assertEquals("column_2", fields.get(1).name());
        assertEquals("column_3", fields.get(2).name());
        assertEquals("wh_file_date", fields.get(3).name());
        assertEquals("wh_file_id", fields.get(4).name());
        assertEquals("wh_row_id", fields.get(5).name());

        final List<JsonNode> jsonList = getJson(result);
        assertEquals(3, jsonList.size());

        JsonNode record = jsonList.get(0);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("hello", record.get("column_1").asText());
        assertEquals("wo\"|rld", record.get("column_2").asText());
        assertEquals("now", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("1", record.get("wh_row_id").asText());

        record = jsonList.get(1);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("foo", record.get("column_1").asText());
        assertEquals("bar", record.get("column_2").asText());
        assertEquals("fizz", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("2", record.get("wh_row_id").asText());

        record = jsonList.get(2);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("fizz", record.get("column_1").asText());
        assertEquals("buzz", record.get("column_2").asText());
        assertEquals("bar", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("3", record.get("wh_row_id").asText());
    }

    @Test
    public void testProcessor_useHeader_noSchema_withQuotesandEscape() throws Exception {
        Path path = Paths.get("src/test/resources/samples/withHeader_withQuotes_withEscape.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_txt_quote", "\"");
        attributes.put("wh_txt_escape", "\\");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
//        System.out.println(prettyPrint(result));

        assertEquals(1, result.size());
        final Schema schema = getSchema(result.get(0));
        final List<Schema.Field> fields = schema.getFields();
        assertEquals(6, fields.size());
        assertEquals("column_1", fields.get(0).name());
        assertEquals("column_2", fields.get(1).name());
        assertEquals("column_3", fields.get(2).name());
        assertEquals("wh_file_date", fields.get(3).name());
        assertEquals("wh_file_id", fields.get(4).name());
        assertEquals("wh_row_id", fields.get(5).name());

        final List<JsonNode> jsonList = getJson(result);
        assertEquals(3, jsonList.size());

        JsonNode record = jsonList.get(0);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("hello", record.get("column_1").asText());
        assertEquals("wo\"|rld", record.get("column_2").asText());
        assertEquals("now", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("1", record.get("wh_row_id").asText());

        record = jsonList.get(1);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("foo", record.get("column_1").asText());
        assertEquals("bar", record.get("column_2").asText());
        assertEquals("fizz", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("2", record.get("wh_row_id").asText());

        record = jsonList.get(2);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("fizz", record.get("column_1").asText());
        assertEquals("buzz", record.get("column_2").asText());
        assertEquals("bar", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("3", record.get("wh_row_id").asText());
    }

    @Test
    public void testProcessor_useHeader_useSchema() throws Exception {
        Path path = Paths.get("src/test/resources/samples/useHeader_useSchema.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_txt_schema", "column_1,column_2,column_3");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
//        System.out.println(prettyPrint(result));

        assertEquals(1, result.size());
        final Schema schema = getSchema(result.get(0));
        final List<Schema.Field> fields = schema.getFields();
        assertEquals(6, fields.size());
        assertEquals("column_1", fields.get(0).name());
        assertEquals("column_2", fields.get(1).name());
        assertEquals("column_3", fields.get(2).name());
        assertEquals("wh_file_date", fields.get(3).name());
        assertEquals("wh_file_id", fields.get(4).name());
        assertEquals("wh_row_id", fields.get(5).name());

        final List<JsonNode> jsonList = getJson(result);
        assertEquals(3, jsonList.size());

        JsonNode record = jsonList.get(0);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("hello", record.get("column_1").asText());
        assertEquals("world", record.get("column_2").asText());
        assertEquals("now", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("1", record.get("wh_row_id").asText());

        record = jsonList.get(1);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("foo", record.get("column_1").asText());
        assertEquals("bar", record.get("column_2").asText());
        assertEquals("fizz", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("2", record.get("wh_row_id").asText());

        record = jsonList.get(2);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("fizz", record.get("column_1").asText());
        assertEquals("buzz", record.get("column_2").asText());
        assertEquals("bar", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("3", record.get("wh_row_id").asText());
    }

    @Test
    public void testProcessor_useHeader_useSchema_extraColsInitial() throws Exception {
        Path path = Paths.get("src/test/resources/samples/useHeader_useSchema_extraCols.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_schema", "column_1,column_2,column_3,column_4");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Expected 4 fields but encountered 3 on row 1"));
    }

    @Test
    public void testProcessor_useHeader_useSchema_extraColsLater() throws Exception {
        Path path = Paths.get("src/test/resources/samples/useHeader_useSchema_extraColsLater.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_schema", "column_1,column_2,column_3");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Expected 3 fields but encountered 4 on row 2"));
    }

    @Test
    public void testProcessor_useHeader_useSchema_headerHasDiffCol() throws Exception {
        Path path = Paths.get("src/test/resources/samples/useHeader_useSchema_diffCol.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_txt_schema", "column_1,column_2,column_3");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Column 'column_4' not in schema."));
    }

    @Test
    public void testProcessor_useHeader_useSchema_schemaHasLessColumns() throws Exception {
        Path path = Paths.get("src/test/resources/samples/useHeader_useSchema.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_txt_schema", "column_1,column_2");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("File has more columns than schema."));
    }

    @Test
    public void testProcessor_useHeader_useSchema_schemaHasMoreColumns() throws Exception {
        Path path = Paths.get("src/test/resources/samples/useHeader_useSchema.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_txt_schema", "column_1,column_2,column_3,column_4");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
//        System.out.println(prettyPrint(result));

        assertEquals(1, result.size());
        final Schema schema = getSchema(result.get(0));
        final List<Schema.Field> fields = schema.getFields();
        assertEquals(7, fields.size());

        assertEquals("column_1", fields.get(0).name());
        assertEquals("column_2", fields.get(1).name());
        assertEquals("column_3", fields.get(2).name());
        assertEquals("column_4", fields.get(3).name());
        assertEquals("wh_file_date", fields.get(4).name());
        assertEquals("wh_file_id", fields.get(5).name());
        assertEquals("wh_row_id", fields.get(6).name());

        final List<JsonNode> jsonList = getJson(result);
        assertEquals(3, jsonList.size());

        JsonNode record = jsonList.get(0);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("column_4"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("hello", record.get("column_1").asText());
        assertEquals("world", record.get("column_2").asText());
        assertEquals("now", record.get("column_3").asText());
        assertEquals("null", record.get("column_4").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("1", record.get("wh_row_id").asText());

        record = jsonList.get(1);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("column_4"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("foo", record.get("column_1").asText());
        assertEquals("bar", record.get("column_2").asText());
        assertEquals("fizz", record.get("column_3").asText());
        assertEquals("null", record.get("column_4").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("2", record.get("wh_row_id").asText());

        record = jsonList.get(2);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("column_4"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("fizz", record.get("column_1").asText());
        assertEquals("buzz", record.get("column_2").asText());
        assertEquals("bar", record.get("column_3").asText());
        assertEquals("null", record.get("column_4").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("3", record.get("wh_row_id").asText());
    }

    @Test
    public void testProcessor_useHeader_useSchema_skipLines() throws Exception {
        Path path = Paths.get("src/test/resources/samples/useHeader_useSchema_skipLines.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_txt_schema", "column_1,column_2,column_3");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
//        System.out.println(prettyPrint(result));

        assertEquals(1, result.size());
        final Schema schema = getSchema(result.get(0));
        final List<Schema.Field> fields = schema.getFields();
        assertEquals(6, fields.size());
        assertEquals("column_1", fields.get(0).name());
        assertEquals("column_2", fields.get(1).name());
        assertEquals("column_3", fields.get(2).name());
        assertEquals("wh_file_date", fields.get(3).name());
        assertEquals("wh_file_id", fields.get(4).name());
        assertEquals("wh_row_id", fields.get(5).name());

        final List<JsonNode> jsonList = getJson(result);
        assertEquals(3, jsonList.size());

        JsonNode record = jsonList.get(0);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("hello", record.get("column_1").asText());
        assertEquals("world", record.get("column_2").asText());
        assertEquals("now", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("1", record.get("wh_row_id").asText());

        record = jsonList.get(1);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("foo", record.get("column_1").asText());
        assertEquals("bar", record.get("column_2").asText());
        assertEquals("fizz", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("2", record.get("wh_row_id").asText());

        record = jsonList.get(2);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("fizz", record.get("column_1").asText());
        assertEquals("buzz", record.get("column_2").asText());
        assertEquals("bar", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("3", record.get("wh_row_id").asText());
    }

    @Test
    public void testProcessor_useHeader_useSchema_skipLines_extraColsInitial() throws Exception {
        Path path = Paths.get("src/test/resources/samples/useHeader_useSchema_skipLines_extraCols.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_schema", "column_1,column_2,column_3,column_4");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Expected 4 fields but encountered 3 on row 1"));
    }

    @Test
    public void testProcessor_useHeader_useSchema_skipLines_extraColsLater() throws Exception {
        Path path = Paths.get("src/test/resources/samples/useHeader_useSchema_skipLines_extraColsLater.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_schema", "column_1,column_2,column_3");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Expected 3 fields but encountered 4 on row 2"));
    }

    @Test
    public void testProcessor_useHeader_useSchema_skipLines_headerHasDiffCol() throws Exception {
        Path path = Paths.get("src/test/resources/samples/useHeader_useSchema_skipLines_diffCol.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_txt_schema", "column_1,column_2,column_3");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("Column 'column_4' not in schema."));
    }

    @Test
    public void testProcessor_useHeader_useSchema_skipLines_schemaHasLessColumns() throws Exception {
        Path path = Paths.get("src/test/resources/samples/useHeader_useSchema_skipLines.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_txt_schema", "column_1,column_2");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        assertEquals(1, runner.getFlowFilesForRelationship("failure").size());
        List<LogMessage> log = runner.getLogger().getErrorMessages();
        assertTrue(log.get(0).getMsg().contains("File has more columns than schema."));
    }

    @Test
    public void testProcessor_useHeader_useSchema_skipLines_schemaHasMoreColumns() throws Exception {
        Path path = Paths.get("src/test/resources/samples/useHeader_useSchema_skipLines.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_txt_schema", "column_1,column_2,column_3,column_4");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
//        System.out.println(prettyPrint(result));

        assertEquals(1, result.size());
        final Schema schema = getSchema(result.get(0));
        final List<Schema.Field> fields = schema.getFields();
        assertEquals(7, fields.size());

        assertEquals("column_1", fields.get(0).name());
        assertEquals("column_2", fields.get(1).name());
        assertEquals("column_3", fields.get(2).name());
        assertEquals("column_4", fields.get(3).name());
        assertEquals("wh_file_date", fields.get(4).name());
        assertEquals("wh_file_id", fields.get(5).name());
        assertEquals("wh_row_id", fields.get(6).name());

        final List<JsonNode> jsonList = getJson(result);
        assertEquals(3, jsonList.size());

        JsonNode record = jsonList.get(0);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("column_4"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("hello", record.get("column_1").asText());
        assertEquals("world", record.get("column_2").asText());
        assertEquals("now", record.get("column_3").asText());
        assertEquals("null", record.get("column_4").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("1", record.get("wh_row_id").asText());

        record = jsonList.get(1);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("column_4"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("foo", record.get("column_1").asText());
        assertEquals("bar", record.get("column_2").asText());
        assertEquals("fizz", record.get("column_3").asText());
        assertEquals("null", record.get("column_4").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("2", record.get("wh_row_id").asText());

        record = jsonList.get(2);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("column_4"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("fizz", record.get("column_1").asText());
        assertEquals("buzz", record.get("column_2").asText());
        assertEquals("bar", record.get("column_3").asText());
        assertEquals("null", record.get("column_4").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("3", record.get("wh_row_id").asText());
    }

    @Test
    public void testProcessor_useHeader_useSchema_skipLines_withQuotesandEscape() throws Exception {
        Path path = Paths.get("src/test/resources/samples/skipLines_withHeader_withQuotes_withEscape.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_schema", "column_1,column_2,column_3");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_txt_skip_lines", "2");
        attributes.put("wh_txt_quote", "\"");
        attributes.put("wh_txt_escape", "\\");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
//        System.out.println(prettyPrint(result));

        assertEquals(1, result.size());
        final Schema schema = getSchema(result.get(0));
        final List<Schema.Field> fields = schema.getFields();
        assertEquals(6, fields.size());
        assertEquals("column_1", fields.get(0).name());
        assertEquals("column_2", fields.get(1).name());
        assertEquals("column_3", fields.get(2).name());
        assertEquals("wh_file_date", fields.get(3).name());
        assertEquals("wh_file_id", fields.get(4).name());
        assertEquals("wh_row_id", fields.get(5).name());

        final List<JsonNode> jsonList = getJson(result);
        assertEquals(3, jsonList.size());

        JsonNode record = jsonList.get(0);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("hello", record.get("column_1").asText());
        assertEquals("wo\"|rld", record.get("column_2").asText());
        assertEquals("now", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("1", record.get("wh_row_id").asText());

        record = jsonList.get(1);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("foo", record.get("column_1").asText());
        assertEquals("bar", record.get("column_2").asText());
        assertEquals("fizz", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("2", record.get("wh_row_id").asText());

        record = jsonList.get(2);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("fizz", record.get("column_1").asText());
        assertEquals("buzz", record.get("column_2").asText());
        assertEquals("bar", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("3", record.get("wh_row_id").asText());
    }

    @Test
    public void testProcessor_useHeader_useSchema_withQuotesandEscape() throws Exception {
        Path path = Paths.get("src/test/resources/samples/withHeader_withQuotes_withEscape.csv");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("wh_txt_use_header", "1");
        attributes.put("wh_txt_schema", "column_1,column_2,column_3");
        attributes.put("wh_txt_delim", "|");
        attributes.put("wh_txt_quote", "\"");
        attributes.put("wh_txt_escape", "\\");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path, attributes);
        runner.run();

        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");

        assertEquals(1, result.size());
        final Schema schema = getSchema(result.get(0));
        final List<Schema.Field> fields = schema.getFields();
        assertEquals(6, fields.size());
        assertEquals("column_1", fields.get(0).name());
        assertEquals("column_2", fields.get(1).name());
        assertEquals("column_3", fields.get(2).name());
        assertEquals("wh_file_date", fields.get(3).name());
        assertEquals("wh_file_id", fields.get(4).name());
        assertEquals("wh_row_id", fields.get(5).name());

        final List<JsonNode> jsonList = getJson(result);
        assertEquals(3, jsonList.size());

        JsonNode record = jsonList.get(0);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("hello", record.get("column_1").asText());
        assertEquals("wo\"|rld", record.get("column_2").asText());
        assertEquals("now", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("1", record.get("wh_row_id").asText());

        record = jsonList.get(1);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("foo", record.get("column_1").asText());
        assertEquals("bar", record.get("column_2").asText());
        assertEquals("fizz", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("2", record.get("wh_row_id").asText());

        record = jsonList.get(2);

        assertTrue(record.has("column_1"));
        assertTrue(record.has("column_2"));
        assertTrue(record.has("column_3"));
        assertTrue(record.has("wh_file_date"));
        assertTrue(record.has("wh_file_id"));
        assertTrue(record.has("wh_row_id"));
        assertEquals("fizz", record.get("column_1").asText());
        assertEquals("buzz", record.get("column_2").asText());
        assertEquals("bar", record.get("column_3").asText());
        assertEquals(attributes.get("wh_file_date"), record.get("wh_file_date").asText());
        assertEquals(attributes.get("wh_file_id"), record.get("wh_file_id").asText());
        assertEquals("3", record.get("wh_row_id").asText());
    }

    public String prettyPrint(final List<MockFlowFile> flowFiles) throws Exception {
        final StringBuilder sb = new StringBuilder();
        for (MockFlowFile flowFile : flowFiles) {
            final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            try (final DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(
                    new SeekableByteArrayInput(flowFile.toByteArray()), datumReader)) {
                for (GenericRecord r : dataFileReader) {
                    final ObjectMapper mapper = new ObjectMapper();
                    final Object jsonObject = mapper.readValue(r.toString(), Object.class);
                    sb.append(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonObject));
                }
            }
        }

        return sb.toString();
    }

    public Schema getSchema(final MockFlowFile flowFile) throws Exception {
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (final DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(
                new SeekableByteArrayInput(flowFile.toByteArray()), datumReader)) {
            return dataFileReader.getSchema();
        }
    }

    public List<JsonNode> getJson(final List<MockFlowFile> flowFiles) throws Exception {
        final List<JsonNode> jsonList = new ArrayList<>();
        for (MockFlowFile flowFile : flowFiles) {
            final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            try (final DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(
                    new SeekableByteArrayInput(flowFile.toByteArray()), datumReader)) {
                for (GenericRecord r : dataFileReader) {
                    final ObjectMapper mapper = new ObjectMapper();
                    final JsonNode readTree = mapper.readTree(r.toString());
                    jsonList.add(readTree);
                }
            }
        }

        return jsonList;
    }
}
