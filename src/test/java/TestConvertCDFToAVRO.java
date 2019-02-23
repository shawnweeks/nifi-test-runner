/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

public class TestConvertCDFToAVRO {

    protected TestRunner runner;
    protected ExecuteGroovyScript proc;

    @Before
    public void init() {
        //init processor
        proc = new ExecuteGroovyScript();
        MockProcessContext context = new MockProcessContext(proc);
        MockProcessorInitializationContext initContext = new MockProcessorInitializationContext(proc, context);
        proc.initialize(initContext);

        assertNotNull(proc.getSupportedPropertyDescriptors());
        runner = TestRunners.newTestRunner(proc);
    }

    @Test
    public void testProcessor() throws IOException {
        String groovyScript = new String(Files.readAllBytes(Paths.get("src/test/resources/groovy/convert_cdf_to_avro.groovy")), "UTF-8");
        runner.setProperty(proc.SCRIPT_BODY, groovyScript);
        try (Stream<Path> paths = Files.walk(Paths.get("E:\\LOGSA\\cdf_sample2"))) {
            paths.filter(Files::isRegularFile).forEach(path -> {
                if (path.getFileName().toString().toLowerCase().endsWith(".cdf")) {
//                    System.out.println("Processing " + path.getFileName());
                    try {
                        runner.enqueue(path);
                        runner.run();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(proc.REL_SUCCESS.getName());

        for (MockFlowFile flowFile : result) {
            System.out.println(flowFile.toString());

            flowFile.getAttributes().forEach((k, v) -> {
                System.out.println(k + ":" + v);
            });
//            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
//            DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new SeekableByteArrayInput(flowFile.toByteArray()), datumReader);
////            System.out.println(dataFileReader.getSchema());
////            System.out.println(new String(dataFileReader.getMeta("avro.codec")));
//            for (GenericRecord r : dataFileReader) {
//                System.out.println(r.toString());
//            }
        }
    }

}
