import jdk.internal.util.xml.impl.Input

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

Schema cdfVarAttSchema = SchemaBuilder
        .record("cdfVarAtt")
        .fields()
        .name("attribute_type").type().stringType().noDefault()
        .name("attribute_value").type().stringType().noDefault()
        .endRecord()

Schema cdfVarSchema = SchemaBuilder
        .record("cdfVar")
//                .namespace("org.apache.hive")
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
//                .namespace("org.apache.hive")
        .fields()
        .name("file_id").type().stringType().noDefault()
        .name("variable_name").type().stringType().noDefault()
        .name("variable_type").type().stringType().noDefault()
        .name("record_number").type().intType().noDefault()
        .name("record_count").type().intType().noDefault()
        .name("record_array").type().array().items().stringType().noDefault()
        .endRecord()

FlowFile flowFile = session.get()

if (!flowFile) return

flowFile.read { inputStream ->
    FlowFile cdfRecFlowFile = session.create(flowFile)

    // Setup CDF Reader
    ByteBuffer byteBuffer = ByteBuffer.wrap ((inputStream as InputStream).getBytes())
    inputStream.close()
    CdfContent cdfContent = new CdfContent(new CdfReader(new SimpleNioBuf(byteBuffer, true, false)))

    DataFileWriter<Record> writer = new DataFileWriter<>(new GenericDatumWriter())
    writer.setCodec(CodecFactory.deflateCodec(9))

    cdfRecFlowFile.write { outputStream ->
        DataFileWriter<Record> w = writer.create cdfVarRecSchema, (outputStream as OutputStream)

        Variable[] vars = cdfContent.getVariables()
        for (Variable var : vars) {
            for (int i = 0; i < var.getRecordCount(); i++) {
                Record r = new Record(cdfVarRecSchema)
                Object tmpArray = var.createRawValueArray()
                Object record = var.readShapedRecord(i, true, tmpArray)

                r.put("file_id", flowFile.getAttribute("uuid"))
                r.put("variable_name", var.getName())
                r.put("variable_type", var.getDataType().getName())
                r.put("record_number", i)

                List<String> recordArray = new ArrayList<>()
                if (record.getClass().isArray()) {
                    int arraySize = Array.getLength(record)
                    r.put("record_count", arraySize)
                    for (int v = 0; v < arraySize; v++) {
                        recordArray.add(Array.get(record, v).toString())
                    }
                } else {
                    r.put("record_count", 1)
                    recordArray.add(record.toString())
                }
                r.put("record_array", recordArray)
                w.append(r)
            }
        }
        w.close()
    }

    REL_SUCCESS << cdfRecFlowFile
}

session.remove(flowFile)