import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.processors.groovyx.flow.SessionFile

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
import uk.ac.bristol.star.cdf.CdfInfo
import uk.ac.bristol.star.cdf.GlobalAttribute
import uk.ac.bristol.star.cdf.Variable
import uk.ac.bristol.star.cdf.VariableAttribute
import uk.ac.bristol.star.cdf.record.SimpleNioBuf

import java.nio.ByteBuffer
import java.lang.reflect.Array

long flowStartTime = System.nanoTime()

Schema cdfAttSchema = SchemaBuilder
		.record("cdfAttRecord")
		.fields()
		.name("attribute_type").type().stringType().noDefault()
		.name("attribute_value").type().stringType().noDefault()
		.endRecord()

Schema cdfGlobalSchema = SchemaBuilder
		.record("cdfGlobalRecord")
		.fields()
		.name("provenance_guid").type().stringType().noDefault()
		.name("majority").type().stringType().noDefault()
		.name("global_attributes").type().map().values().array().items(cdfAttSchema).noDefault()
		.name("archive_path").type().stringType().noDefault()
		.name("archive_filename").type().stringType().noDefault()
		.name("original_filename").type().stringType().noDefault()
		.name("original_file_size").type().longType().noDefault()
		.name("processed_date").type().stringType().noDefault()		
		// Partition Values
		.name("make").type().stringType().noDefault()
		.name("model").type().stringType().noDefault()
		.name("file_date").type().stringType().noDefault()
		.endRecord()

Schema cdfVarSchema = SchemaBuilder
        .record("cdfVarRecord")
        .fields()
        .name("provenance_guid").type().stringType().noDefault()
        .name("variable_name").type().stringType().noDefault()
        .name("variable_type").type().stringType().noDefault()
        .name("num_elements").type().intType().noDefault()
        .name("dim").type().intType().noDefault()
        .name("dim_sizes").type().array().items().intType().noDefault()
        .name("dim_variances").type().array().items().booleanType().noDefault()
        .name("rec_variance").type().intType().noDefault()
        .name("max_records").type().intType().noDefault()
        .name("attributes").type().map().values(cdfAttSchema).noDefault()
		// Partition Values
		.name("make").type().stringType().noDefault()
		.name("model").type().stringType().noDefault()
		.name("file_date").type().stringType().noDefault()
		.endRecord()


Schema cdfVarRecSchema = SchemaBuilder
		.record("cdfVarRecRecord")
		.fields()
		.name("file_id").type().stringType().noDefault()
		.name("variable_name").type().stringType().noDefault()
		.name("variable_type").type().stringType().noDefault()
		.name("variable_attributes").type().map().values(cdfAttSchema).noDefault()
		.name("record_number").type().intType().noDefault()
		.name("record_size").type().intType().noDefault()
		.name("record_array").type().array().items().stringType().noDefault()
		.endRecord()

SessionFile flowFile = session.get()

flowFile.session

if (!flowFile) return

long startTime

CdfContent cdfContent
startTime = System.nanoTime()

// Populate ByteBuffer from incoming FlowFile.
ByteBuffer byteBuffer
flowFile.read { InputStream inputStream ->
	byteBuffer = ByteBuffer.wrap inputStream.bytes
	inputStream.close()
}

// Setup CDF Reader
try {
	cdfContent = new CdfContent(new CdfReader(new SimpleNioBuf(byteBuffer, true, false)))
} catch (IOException e) {
	log.error "Failed to read CDF File", e
	REL_FAILURE << flowFile
	return // No reason to continue if we can't read CDF File.
}

double cdfReadTime
double cdfWriteTime

cdfReadTime = (System.nanoTime() - startTime) / 1000.00 / 1000.00 / 1000.00

DataFileWriter<Record> writer = new DataFileWriter<>(new GenericDatumWriter())
writer.setCodec CodecFactory.deflateCodec(5)


SessionFile cdfGlobalFlowFile = session.create flowFile
cdfGlobalFlowFile."cdf_read_time" = cdfReadTime
cdfGlobalFlowFile."cdf_extract_type" = "cdf_global"
cdfGlobalFlowFile."mime.type" = "application/avro-binary"

startTime = System.nanoTime()

cdfGlobalFlowFile.write { OutputStream outputStream ->
	DataFileWriter<Record> w = writer.create cdfGlobalSchema, outputStream
	Record r = new Record(cdfGlobalSchema)
	r.put "provenance_guid", flowFile."provenance_guid"
	
	CdfInfo cdfInfo = cdfContent.getCdfInfo()
	r.put "majority", cdfInfo.getRowMajor() ? "ROW" : "COLUMN"
	
	Map<String,List<Record>> globalAttributeMap = new HashMap<>()
	cdfContent.getGlobalAttributes().each{ GlobalAttribute globalAttribute ->
		List<Record> attributeEntries = new ArrayList<>(globalAttribute.getEntries().length)
		globalAttribute.getEntries().each { AttributeEntry attributeEntry ->
			Record r1 = new Record(cdfAttSchema)
			r1.put "attribute_type", attributeEntry.dataType.name
			r1.put "attribute_value", attributeEntry.toString()
			attributeEntries.add r1
		}
		globalAttributeMap.put globalAttribute.getName(), attributeEntries
	}
	
	r.put "global_attributes", globalAttributeMap
	r.put "archive_path", flowFile."archive_path"
	r.put "archive_filename", flowFile."archive_filename"
	r.put "original_filename", flowFile."filename"
	r.put "original_file_size", flowFile.getSize()
	r.put "processed_date", flowFile."processed_date"
	r.put "make", flowFile."make"
	r.put "model", flowFile."model"
	r.put "file_date", flowFile."file_date"
	w.append r
	w.close()
}
cdfWriteTime = (System.nanoTime() - startTime) / 1000.00 / 1000.00 / 1000.00
cdfGlobalFlowFile."cdf_write_time" = cdfWriteTime
cdfGlobalFlowFile."cdf_total_time" = (System.nanoTime() - flowStartTime) / 1000.00 / 1000.00 / 1000.00
REL_SUCCESS << cdfGlobalFlowFile

SessionFile cdfRecFlowFile = session.create flowFile
cdfRecFlowFile."cdf_read_time" = cdfReadTime
cdfRecFlowFile."cdf_extract_type" = "cdf_variable_record"
cdfRecFlowFile."mime.type" = "application/avro-binary"


startTime = System.nanoTime()

cdfRecFlowFile.write { OutputStream outputStream ->
	DataFileWriter<Record> w = writer.create cdfVarRecSchema, outputStream

	cdfContent.getVariables().each { Variable var ->
		String uuid = flowFile."uuid"
		String variableName = var.name
		String variableType = var.dataType.name

		Map<String, Record> variableAttributes = new HashMap<>()
		cdfContent.variableAttributes.each { VariableAttribute variableAttribute ->
			AttributeEntry attributeEntry = variableAttribute.getEntry var
			if (attributeEntry != null) {
				Record r = new Record(cdfAttSchema)
				r.put "attribute_type", attributeEntry.dataType.name
				r.put "attribute_value", attributeEntry.toString()
				variableAttributes.put variableAttribute.name, r
			}
		}

		for (int i = 0; i < var.recordCount; i++) {
			Record r = new Record(cdfVarRecSchema)
			Object tmpArray = var.createRawValueArray()
			var.readRawRecord i, tmpArray

			r.put "file_id", uuid
			r.put "variable_name", variableName
			r.put "variable_type", variableType
			r.put "variable_attributes", variableAttributes
			r.put "record_number", i + 1

			List<Object> recordArray
			if (tmpArray.getClass().isArray()) {
				int arraySize = Array.getLength tmpArray
				r.put "record_size", arraySize

				recordArray = new ArrayList<>(arraySize)
				for (int x = 0; x < arraySize; x++) {
					recordArray.add Array.get(tmpArray, x).toString()
				}
			} else {
				r.put "record_size", 1
				recordArray = new ArrayList<>(1)
				recordArray.add tmpArray.toString()
			}
			r.put "record_array", recordArray
			w.append r
		}
	}
	w.close()
}

cdfWriteTime = (System.nanoTime() - startTime) / 1000.00 / 1000.00 / 1000.00
cdfRecFlowFile."cdf_write_time" = cdfWriteTime
cdfRecFlowFile."cdf_total_time" = (System.nanoTime() - flowStartTime) / 1000.00 / 1000.00 / 1000.00
REL_SUCCESS << cdfRecFlowFile

// Remove Original File
session.remove flowFile