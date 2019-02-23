import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processors.groovyx.flow.SessionFile

import java.lang.reflect.Array;
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericDatumWriter
import uk.ac.bristol.star.cdf.AttributeEntry
import uk.ac.bristol.star.cdf.CdfContent
import uk.ac.bristol.star.cdf.CdfReader
import uk.ac.bristol.star.cdf.Variable
import uk.ac.bristol.star.cdf.VariableAttribute
import uk.ac.bristol.star.cdf.record.SimpleNioBuf

import java.nio.ByteBuffer

long flowStartTime = System.nanoTime()

Schema cdfVarAttSchema = SchemaBuilder
        .record("cdfVarAtt")
        .fields()
        .name("attribute_type").type().stringType().noDefault()
        .name("attribute_value").type().stringType().noDefault()
        .endRecord()

Schema cdfVarSchema = SchemaBuilder
        .record("cdfVar")
        .fields()
        .name("file_id").type().stringType().noDefault()
        .name("variable_name").type().stringType().noDefault()
        .name("variable_type").type().stringType().noDefault()
//                .name("num_elements").type().intType().noDefault()
//                .name("dim").type().intType().noDefault()
//                .name("dim_sizes").type().array().items().intType().noDefault()
//                .name("dim_variances").type().array().items().booleanType().noDefault()
//                .name("rec_variance").type().intType().noDefault()
//                .name("max_records").type().intType().noDefault()
        .name("attributes").type().map().values(cdfVarAttSchema).noDefault()
        .endRecord()


Schema cdfVarRecSchema = SchemaBuilder
        .record("cdfVarRec")
        .fields()
        .name("file_id").type().stringType().noDefault()
        .name("variable_name").type().stringType().noDefault()
        .name("variable_type").type().stringType().noDefault()
        .name("record_number").type().intType().noDefault()
        .name("record_count").type().intType().noDefault()
        .name("record_array").type().array().items().stringType().noDefault()
        .endRecord()

SessionFile flowFile = session.get()

if (!flowFile) return

long startTime

CdfContent cdfContent
startTime = System.nanoTime()
try {
    flowFile.read { inputStream ->

        // Setup CDF Reader
        ByteBuffer byteBuffer = ByteBuffer.wrap((inputStream as InputStream).getBytes())
        inputStream.close()
        cdfContent = new CdfContent(new CdfReader(new SimpleNioBuf(byteBuffer, true, false)))
    }
} catch (IOException e) {
    log.error(e)
    REL_FAILURE << flowFile
}
double cdfReadTime = (System.nanoTime() - startTime) / 1000.00 / 1000.00 / 1000.00


SessionFile cdfRecFlowFile = session.create(flowFile)

cdfRecFlowFile."cdf_read_time" = cdfReadTime
cdfRecFlowFile."cdf_extract_type" = "cdfRecFlowFile"
cdfRecFlowFile."mime.type" = "application/avro-binary"

DataFileWriter<Record> writer = new DataFileWriter<>(new GenericDatumWriter())
//writer.setCodec(CodecFactory.deflateCodec(1))

startTime = System.nanoTime()

cdfRecFlowFile.write { OutputStream outputStream ->
    DataFileWriter<Record> w = writer.create cdfVarRecSchema, outputStream

    cdfContent.getVariables().each { var ->
        for (int i = 0; i < var.getRecordCount(); i++) {
            Record r = new Record(cdfVarRecSchema)
            Object tmpArray = var.createRawValueArray()
            var.readRawRecord i, tmpArray

            r.put "file_id", flowFile.getAttribute("uuid")
            r.put "variable_name", var.getName()
            r.put "variable_type", var.getDataType().getName()
            r.put "record_number", i + 1

            List<String> recordArray
            if (tmpArray.getClass().isArray()) {
                int arraySize = Array.getLength tmpArray
                r.put("record_count", arraySize)

                recordArray = new ArrayList<>(arraySize)
                for (int x = 0; i < arraySize; i++) {
                    recordArray.add Array.get(tmpArray, x).toString()
                }
            } else {
                r.put("record_count", 1)
                recordArray = new ArrayList<>(1)
                recordArray.add tmpArray.toString()
            }
            r.put("record_array", recordArray)
            w.append(r)
        }
    }
    w.close()
}

double cdfWriteTime = (System.nanoTime() - startTime) / 1000.00 / 1000.00 / 1000.00
cdfRecFlowFile."cdf_write_time" = cdfWriteTime
cdfRecFlowFile."cdf_total_time" = (System.nanoTime() - flowStartTime) / 1000.00 / 1000.00 / 1000.00

REL_SUCCESS << cdfRecFlowFile

session.remove(flowFile)