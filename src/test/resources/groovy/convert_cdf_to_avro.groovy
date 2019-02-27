import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processors.groovyx.flow.GroovySessionFile

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileWriter
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
		.name("year").type().intType().noDefault()
		.name("month").type().intType().noDefault()
		.name("day").type().intType().noDefault()
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
		.name("rec_variance").type().booleanType().noDefault()
		.name("max_records").type().intType().noDefault()
		.name("attributes").type().map().values(cdfAttSchema).noDefault()
		// Partition Values
		.name("make").type().stringType().noDefault()
		.name("model").type().stringType().noDefault()
		.name("year").type().intType().noDefault()
		.name("month").type().intType().noDefault()
		.name("day").type().intType().noDefault()
		.endRecord()


Schema cdfVarRecSchema = SchemaBuilder
		.record("cdfVarRecRecord")
		.fields()
		.name("provenance_guid").type().stringType().noDefault()
		.name("variable_name").type().stringType().noDefault()
		.name("record_number").type().intType().noDefault()
		.name("record_array").type().array().items().stringType().noDefault()
		// Partition Values
		.name("make").type().stringType().noDefault()
		.name("model").type().stringType().noDefault()
		.name("year").type().intType().noDefault()
		.name("month").type().intType().noDefault()
		.name("day").type().intType().noDefault()
		.endRecord()

GroovySessionFile flowFile = session.get()
ComponentLog LOGGER = log


if (!flowFile) return

	long startTime

CdfContent cdfContent
startTime = System.nanoTime()

// Setup CDF Reader
try {
	// Populate ByteBuffer from incoming FlowFile.
	ByteBuffer byteBuffer
	flowFile.read { InputStream inputStream ->
		byteBuffer = ByteBuffer.wrap inputStream.bytes
		cdfContent = new CdfContent(new CdfReader(new SimpleNioBuf(byteBuffer, true, false)))
		inputStream.close()
	}
} catch (Exception e) {
	LOGGER.error "Failed to read CDF File", e
	REL_FAILURE << flowFile
	return // No reason to continue if we can't read CDF File.
}

double cdfReadTime
double cdfWriteTime

cdfReadTime = (System.nanoTime() - startTime) / 1000.00 / 1000.00 / 1000.00

DataFileWriter<Record> writer = new DataFileWriter<>(new GenericDatumWriter())
writer.setCodec CodecFactory.deflateCodec(5)


GroovySessionFile cdfGlobalFlowFile = session.create flowFile
cdfGlobalFlowFile."cdf_read_time" = cdfReadTime
cdfGlobalFlowFile."cdf_extract_type" = "cdf_global"
cdfGlobalFlowFile."mime.type" = "application/avro-binary"

startTime = System.nanoTime()

cdfGlobalFlowFile.write { OutputStream outputStream ->
	DataFileWriter<Record> w = writer.create cdfGlobalSchema, outputStream
	Record r = new Record(cdfGlobalSchema)
	Map<String, List<Record>> globalAttributeMap = new HashMap<>()
	cdfContent.getGlobalAttributes().each { GlobalAttribute globalAttribute ->
		List<Record> attributeEntries = new ArrayList<>(globalAttribute.getEntries().length)
		globalAttribute.getEntries().each { AttributeEntry attributeEntry ->
			Record r1 = new Record(cdfAttSchema)
			r1.put "attribute_type", attributeEntry.dataType.name
			r1.put "attribute_value", attributeEntry.toString()
			attributeEntries.add r1
		}
		globalAttributeMap.put globalAttribute.getName(), attributeEntries
	}

	r.put "provenance_guid", flowFile."provenance_guid"
	r.put "majority", cdfContent.cdfInfo.rowMajor ? "ROW" : "COLUMN"
	r.put "global_attributes", globalAttributeMap
	r.put "archive_path", flowFile."archive_path"
	r.put "archive_filename", flowFile."archive_filename"
	r.put "original_filename", flowFile."filename"
	r.put "original_file_size", flowFile.getSize()
	r.put "processed_date", flowFile."processed_date"
	r.put "make", flowFile."make"
	r.put "model", flowFile."model"
	r.put "year", flowFile."year" as Integer
	r.put "month", flowFile."month" as Integer
	r.put "day", flowFile."day" as Integer
	w.append r
	w.close()
}
cdfWriteTime = (System.nanoTime() - startTime) / 1000.00 / 1000.00 / 1000.00
cdfGlobalFlowFile."cdf_write_time" = cdfWriteTime
cdfGlobalFlowFile."cdf_total_time" = (System.nanoTime() - flowStartTime) / 1000.00 / 1000.00 / 1000.00
REL_SUCCESS << cdfGlobalFlowFile

//CDF Variables
GroovySessionFile cdfVarFlowFile = session.create flowFile
cdfVarFlowFile."cdf_read_time" = cdfReadTime
cdfVarFlowFile."cdf_extract_type" = "cdf_variable"
cdfVarFlowFile."mime.type" = "application/avro-binary"

startTime = System.nanoTime()
cdfVarFlowFile.write { OutputStream outputStream ->
	DataFileWriter<Record> w = writer.create cdfVarSchema, outputStream

	cdfContent.getVariables().each { Variable var ->
		Record r = new Record(cdfVarSchema)
		Map<String, Record> variableAttributes = new HashMap<>()
		cdfContent.variableAttributes.each { VariableAttribute variableAttribute ->
			AttributeEntry attributeEntry = variableAttribute.getEntry var
			if (attributeEntry != null) {
				Record r1 = new Record(cdfAttSchema)
				r1.put "attribute_type", attributeEntry.dataType.name
				r1.put "attribute_value", attributeEntry.toString()
				variableAttributes.put variableAttribute.name, r1
			}
		}
		r.put "provenance_guid", flowFile."provenance_guid"
		r.put "variable_name", var.name
		r.put "variable_type", var.dataType.name
		r.put "num_elements", var.descriptor.numElems
		r.put "dim", var.descriptor.zNumDims
		r.put "dim_sizes", var.descriptor.zDimSizes.toList()
		r.put "dim_variances", var.descriptor.dimVarys.toList()
		r.put "rec_variance", var.recordVariance
		r.put "max_records", var.descriptor.maxRec
		r.put "attributes", variableAttributes
		r.put "make", flowFile."make"
		r.put "model", flowFile."model"
		r.put "year", flowFile."year" as Integer
		r.put "month", flowFile."month" as Integer
		r.put "day", flowFile."day" as Integer
		w.append r
	}
	w.close()
}

cdfWriteTime = (System.nanoTime() - startTime) / 1000.00 / 1000.00 / 1000.00
cdfVarFlowFile."cdf_write_time" = cdfWriteTime
cdfVarFlowFile."cdf_total_time" = (System.nanoTime() - flowStartTime) / 1000.00 / 1000.00 / 1000.00
REL_SUCCESS << cdfVarFlowFile

// CDF Records
GroovySessionFile cdfRecFlowFile = session.create flowFile
cdfRecFlowFile."cdf_read_time" = cdfReadTime
cdfRecFlowFile."cdf_extract_type" = "cdf_variable_record"
cdfRecFlowFile."mime.type" = "application/avro-binary"

startTime = System.nanoTime()
cdfRecFlowFile.write { OutputStream outputStream ->
	DataFileWriter<Record> w = writer.create cdfVarRecSchema, outputStream

	cdfContent.getVariables().each { Variable var ->

		for (int i = 0; i < var.recordCount; i++) {
			Record r = new Record(cdfVarRecSchema)
			Object tmpArray = var.createRawValueArray()
			var.readRawRecord i, tmpArray
			List<Object> recordArray
			if (tmpArray.getClass().isArray()) {
				int arraySize = Array.getLength tmpArray
				recordArray = new ArrayList<>(arraySize)
				for (int x = 0; x < arraySize; x++) {
					recordArray.add Array.get(tmpArray, x).toString()
				}
			} else {
				recordArray = new ArrayList<>(1)
				recordArray.add tmpArray.toString()
			}
			r.put "provenance_guid", flowFile."provenance_guid"
			r.put "variable_name", var.name
			r.put "record_number", i
			r.put "record_array", recordArray
			r.put "make", flowFile."make"
			r.put "model", flowFile."model"
			r.put "year", flowFile."year" as Integer
			r.put "month", flowFile."month" as Integer
			r.put "day", flowFile."day" as Integer
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