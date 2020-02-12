import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.commons.csv.CSVRecord
import org.apache.nifi.controller.AbstractControllerService
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.schema.access.SchemaNotFoundException
import org.apache.nifi.serialization.MalformedRecordException
import org.apache.nifi.serialization.RecordReader
import org.apache.nifi.serialization.RecordReaderFactory
import org.apache.nifi.serialization.SimpleRecordSchema
import org.apache.nifi.serialization.record.MapRecord
import org.apache.nifi.serialization.record.Record
import org.apache.nifi.serialization.record.RecordField
import org.apache.nifi.serialization.record.RecordFieldType
import org.apache.nifi.serialization.record.RecordSchema

class CSVRecordReader implements RecordReader {
    Map<String, String> variables
    InputStream inputStream
    long inputLength
    ComponentLog logger

    private RecordSchema schema

    private Reader reader
    private Iterator<Object> iterator
    private long rowCounter
    private boolean firstRecord
    private int fieldCount

    CSVRecordReader(Map<String, String> variables,
                    InputStream inputStream,
                    long inputLength,
                    ComponentLog logger) {
        println "Printing Variables"
        for(String var : variables){
            println var
        }

        this.variables = variables
        this.inputStream = inputStream
        this.inputLength = inputLength
        this.logger = logger
        this.schema = getSchemaFromDelimitedString(variables.get("wh_csv_schema"))

        CSVFormat format = CSVFormat.newFormat('|' as char).withFirstRecordAsHeader()
        reader = new InputStreamReader(inputStream,'UTF-8')
        iterator = CSVParser.parse(reader,format).iterator()
        rowCounter = 0
        firstRecord = true
    }

    @Override
    Record nextRecord(boolean b, boolean b1) throws IOException, MalformedRecordException {
        if(iterator.hasNext()){
            ++rowCounter
            CSVRecord record = iterator.next() as CSVRecord
            if(firstRecord){
                fieldCount = record.size()
                firstRecord = false
            }
            if(fieldCount != record.size()){
                throw new MalformedRecordException("Expected " + fieldCount + " fields but encountered " + record.size() + " on row " + rowCounter)
            }
            Map<String, Object> recordMap = record.toMap()
            recordMap.put("wh_file_date",variables.get("wh_file_date"))
            recordMap.put("wh_file_id",variables.get("wh_file_id"))
            recordMap.put("wh_row_id",rowCounter)
            return new MapRecord(schema,recordMap)
        }
        return null
    }

    @Override
    RecordSchema getSchema() throws MalformedRecordException {
        return schema
    }

    @Override
    void close() throws IOException {
        reader.close()
    }

    private RecordSchema getSchemaFromDelimitedString(String schemaString){
        List<RecordField> fields = new ArrayList<>()
        for(String field : schemaString.split(',')){
            fields.add(new RecordField(field, RecordFieldType.STRING.dataType,true))
        }
        fields.add(new RecordField("wh_file_date",RecordFieldType.LONG.dataType))
        fields.add(new RecordField("wh_file_id",RecordFieldType.STRING.dataType))
        fields.add(new RecordField("wh_row_id",RecordFieldType.TIMESTAMP.dataType))
        return new SimpleRecordSchema(fields)
    }
}

class CSVRecordReaderFactory extends AbstractControllerService implements RecordReaderFactory {


    RecordReader createRecordReader(Map<String, String> variables,
                                    InputStream inputStream,
                                    long inputLength,
                                    ComponentLog logger) throws MalformedRecordException, IOException, SchemaNotFoundException {
        return new CSVRecordReader(variables,inputStream,inputLength,logger)
    }
}

reader = new CSVRecordReaderFactory()