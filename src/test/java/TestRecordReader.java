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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TestRecordReader {

    private TestRunner runner;
    private ConvertRecord processor;
    private ScriptedReader reader;
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

    @Test
    public void testProcessor() throws IOException {
        Path path = Paths.get("src/test/resources/samples/sample.csv");
        Map<String,String> attributes = new HashMap<>();
        attributes.put("wh_csv_schema","column_1,column_2,column_3");
        attributes.put("wh_file_date", String.valueOf(System.currentTimeMillis()));
        attributes.put("wh_file_id", UUID.randomUUID().toString());
        runner.enqueue(path,attributes);
        runner.run();

        MockFlowFile flowFile = runner.getFlowFilesForRelationship("failure").get(0);
        System.out.println(flowFile.toString());
        for (String keys : flowFile.getAttributes().keySet())
        {
            System.out.println(keys + ":" + flowFile.getAttribute(keys));
        }
    }

    @After
    public void printResults() throws IOException {
        System.out.println("Printing Results");
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");

        for (MockFlowFile flowFile : result) {
            System.out.println(flowFile.toString());
            System.out.println(flowFile.getSize());

            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(
                    new SeekableByteArrayInput(flowFile.toByteArray()), datumReader)) {
                int recordCount = 0;
                for (GenericRecord r : dataFileReader) {
                    ObjectMapper mapper = new ObjectMapper();
                    Object jsonObject = mapper.readValue(r.toString(), Object.class);
                    String prettyJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonObject);
                    System.out.println(prettyJson);
                    recordCount++;
                }
                System.out.println(recordCount + " Record(s)");
            }
        }
    }
}
