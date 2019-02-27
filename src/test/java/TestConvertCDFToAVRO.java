import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.nifi.processors.groovyx.ExecuteGroovyScript;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockProcessorInitializationContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
//import java.util.TreeMap;
import java.util.stream.Stream;

public class TestConvertCDFToAVRO {

	protected TestRunner runner;
	protected ExecuteGroovyScript proc;

	@Before
	public void init() {
		proc = new ExecuteGroovyScript();
		MockProcessContext context = new MockProcessContext(proc);
		MockProcessorInitializationContext initContext = new MockProcessorInitializationContext(proc, context);
		proc.initialize(initContext);
		assertNotNull(proc.getSupportedPropertyDescriptors());
		runner = TestRunners.newTestRunner(proc);
	}

	@Test
	public void testProcessor() throws IOException {
		String groovyScript = new String(
				Files.readAllBytes(Paths.get("src/test/resources/groovy/convert_cdf_to_avro.groovy")), "UTF-8");
		runner.setProperty(ExecuteGroovyScript.SCRIPT_BODY, groovyScript);
		try (Stream<Path> paths = Files.walk(Paths.get("src/test/resources/cdf"))) {
			paths.filter(Files::isRegularFile).forEach(path -> {
				if (path.getFileName().toString().toLowerCase().endsWith(".cdf")) {
//                    System.out.println("Processing " + path.getFileName());
					try {
						Map<String,String> atts = new HashMap<>();
						atts.put("fileSize", String.valueOf(Files.size(path)));
						atts.put("provenance_guid", java.util.UUID.randomUUID().toString());
						atts.put("archive_path", "/archive/cbm");
						atts.put("archive_filename", path.getFileName() + ".gz");
						atts.put("processed_date", "9999-01-01");						
						atts.put("make", "TANK");
						atts.put("model", "LARGE");
						atts.put("file_date", "2000-01-01");
						runner.enqueue(path).putAttributes(atts);
						runner.run();
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			});
		}
		final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_SUCCESS.getName());

		for (MockFlowFile flowFile : result) {
			System.out.println(flowFile.toString());

//			new TreeMap<>(flowFile.getAttributes()).forEach((k, v) -> {
//				System.out.println(k + ":" + v);
//			});
			DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
			try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(
					new SeekableByteArrayInput(flowFile.toByteArray()), datumReader)) {
//				System.out.println(dataFileReader.getSchema().toString(true));
//            System.out.println(new String(dataFileReader.getMeta("avro.codec")));
				for (GenericRecord r : dataFileReader) {
//                System.out.println(r.toString());
					ObjectMapper mapper = new ObjectMapper();
					Object jsonObject = mapper.readValue(r.toString(), Object.class);
					String prettyJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonObject);
					System.out.println(prettyJson);
				}
			}
		}
	}

}
