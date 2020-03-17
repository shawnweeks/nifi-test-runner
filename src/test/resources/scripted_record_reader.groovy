import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.MappingIterator
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.csv.CsvMapper
import com.fasterxml.jackson.dataformat.csv.CsvSchema
import com.fasterxml.jackson.dataformat.csv.CsvParser
import com.fasterxml.jackson.databind.DeserializationFeature
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
import java.util.LinkedHashMap

class CSVRecordReader implements RecordReader {
    private Map<String, String> variables
    private InputStream inputStream
    private long inputLength
    private ComponentLog logger

    private RecordSchema schema

    private BufferedReader buffReader
    private MappingIterator<Map<String, Object>> iterator
    private long rowCounter
    private boolean firstRecord
    private int fieldCount
    private String delimiter
    private List<String> headerNames
    private boolean hasSchema
    private boolean useHeaders 
    
    public static enum CsvAttribute {
        SCHEMA("wh_txt_schema"),
        DELIM("wh_txt_delim"),
        ESCAPE("wh_txt_escape"),
        QUOTE("wh_txt_quote"),
        USE_HEADER("wh_txt_use_header"),
        SKIP_LINES("wh_txt_skip_lines"),
        FILE_DATE("wh_file_date"),
        FILE_ID("wh_file_id")

        private final String attribute

        private CsvAttribute(final String attribute) {
            this.attribute = attribute
        }

        public String getAttribute() {
            return this.attribute
        }
    }

    CSVRecordReader(final Map<String, String> variables,
        final InputStream inputStream,
        final long inputLength,
        final ComponentLog logger) {
        println "Printing Variables"
        for(String var : variables){
            println var
        }

        this.variables = variables
        this.inputStream = inputStream
        this.inputLength = inputLength
        this.logger = logger

        final String schemaAttribute = variables.get(CsvAttribute.SCHEMA.getAttribute())
        if (null != schemaAttribute && !schemaAttribute.trim().isEmpty()) {
            this.hasSchema = true
        } else {
            this.hasSchema = false
        }
        
        final String useHeaderAttribute = variables.get(CsvAttribute.USE_HEADER.getAttribute())
        if (null != useHeaderAttribute && !useHeaderAttribute.trim().isEmpty()) {
            if (useHeaderAttribute.equalsIgnoreCase("y")) {
                this.useHeaders = true
            } else if (useHeaderAttribute.equalsIgnoreCase("n")) {
                this.useHeaders = false
            } else {
                final String message = "wh_txt_use_header has an unknown value: \"" + useHeaderAttribute + "\""
                logger.error(message)
                throw new MalformedRecordException(message)
            }
        } else {
            this.useHeaders = false
        }
  
        this.delimiter = variables.get(CsvAttribute.DELIM.getAttribute())
        if (null != this.delimiter && this.delimiter.equalsIgnoreCase("")) {
            this.delimiter = null
        }
        String quote = variables.get(CsvAttribute.QUOTE.getAttribute())
        if (null != quote && quote.trim().isEmpty()) {
            quote = null
        }
        String escape = variables.get(CsvAttribute.ESCAPE.getAttribute())
        if (null != escape && escape.trim().isEmpty()) {
            escape = null
        }
        
        int skipLines = 0;
        final String skipLinesAttribute = variables.get(CsvAttribute.SKIP_LINES.getAttribute())
        if (null != skipLinesAttribute && !skipLinesAttribute.trim().isEmpty()) {
            skipLines = Integer.parseInt(skipLinesAttribute)
        }
        
        final InputStreamReader inputStreamReader = new InputStreamReader(inputStream,'UTF-8')
        this.buffReader = new BufferedReader(inputStreamReader)
        
        if (skipLines > 0) {
            logger.info("Skipping " + skipLines + " lines.")
        }
        for (int i = 0; i < skipLines; i++) {
            if (logger.isDebugEnabled()) {
                logger.debug(buffReader.readLine())
            } else {
                buffReader.readLine()
            }
        }

        if (null != delimiter) {
            
            if (!useHeaders && !hasSchema) {
                final String message = "Useheader or schema must be provided."
                logger.error(message)
                throw new MalformedRecordException(message)
            }
            
            if (delimiter.length() < 2) {
                CsvSchema.Builder format = CsvSchema.builder()
                .setColumnSeparator(delimiter as char)
                
                CsvMapper mapper = new CsvMapper()
                if (null != quote) {
                    format = format.setQuoteChar(quote as char)
                }
        
                if (null != escape) {
                    format = format.setEscapeChar(escape as char)
                }
                
                if (useHeaders) {
                    mapper = mapper.enable(CsvParser.Feature.FAIL_ON_MISSING_COLUMNS)
                    format = format.setUseHeader(true)
                    if (hasSchema) {
                        this.schema = getSchemaFromList(splitStringToList(variables.get(CsvAttribute.SCHEMA.getAttribute()), ","))
                    } 
                } else if (hasSchema) {
                    this.headerNames = splitStringToList(variables.get(CsvAttribute.SCHEMA.getAttribute()), ",")
                    this.schema = getSchemaFromList(this.headerNames)
                    
                    for (String header : headerNames) {
                        format = format.addColumn(header)
                    }
                }
        
                this.iterator = mapper.readerFor(Map.class).with(format.build()).readValues(buffReader)

                if (useHeaders) {
                    final CsvSchema csvSchema = this.iterator.getParserSchema()
                    final Iterator<CsvSchema.Column> colIter = csvSchema.iterator();
                    this.headerNames = new ArrayList<>()
                    while(colIter.hasNext()) {
                        this.headerNames.add(colIter.next().getName())
                    }
                    
                    if (!hasSchema) {
                        this.schema = getSchemaFromList(headerNames)
                    } else {
                        checkSchema(variables.get(CsvAttribute.SCHEMA.getAttribute()).split(','), headerNames)
                    }
                }
            } else {
                // Multi-char delimiter.
                if (useHeaders) {
                    final String line = buffReader.readLine()
                    this.headerNames = splitStringToList(line, delimiter)
                    
                    if (hasSchema) {
                        checkSchema(variables.get(CsvAttribute.SCHEMA.getAttribute()).split(','), this.headerNames)
                        this.schema = getSchemaFromList(splitStringToList(variables.get(CsvAttribute.SCHEMA.getAttribute()), ","))
                    } else {
                        this.schema = getSchemaFromList(this.headerNames)
                    }
                } else if (hasSchema) {
                    this.headerNames = splitStringToList(variables.get(CsvAttribute.SCHEMA.getAttribute()), ",")
                    this.schema = getSchemaFromList(this.headerNames)
                }
            }
        } else {
            this.schema = getNoDelimiterSchema()
        }

        this.rowCounter = 0
        this.firstRecord = true
    }
        
    private void checkSchema(final String[] recordSchema, final List<String> colSubset) {
        if (recordSchema.length < colSubset.size()) {
            final String message = "File has more columns than schema."
            logger.error(message)
            throw new MalformedRecordException(message)
        }

        for (String col : colSubset) {
            boolean found = false
            for (String schemaCol : recordSchema) {
                if (col.equalsIgnoreCase(schemaCol)) {
                    found = true
                    break
                }
            }

            if (!found) {
                final String message = "Column '" + col + "' not in schema."
                logger.error(message)
                throw new MalformedRecordException(message)
            }
        }
    }
    
    Record processBufferedRecord() {
        final String record = buffReader.readLine()
        if (null == record) {
            return null
        }
        ++rowCounter
        final Map<String, Object> recordMap = new HashMap<>()
        recordMap.put("row_data", record)
        recordMap.put("wh_file_date",variables.get(CsvAttribute.FILE_DATE.getAttribute()))
        recordMap.put("wh_file_id",variables.get(CsvAttribute.FILE_ID.getAttribute()))
        recordMap.put("wh_row_id",rowCounter)
        final MapRecord mapRecord = new MapRecord(schema,recordMap)
        return mapRecord
    }
    
    Record processRecord_multiDelimiter() {
        final String line = buffReader.readLine()
        if (null == line) {
            return null
        }
        ++rowCounter
        final String[] record = line.split(delimiter)
        if(firstRecord){
            if (!useHeaders && hasSchema) {
                fieldCount = record.length
                if (record.length > headerNames.size()) {
                    final String message = "File has more columns than schema."
                    throw new MalformedRecordException(message)
                }
            } 
            else {
                fieldCount = headerNames.size()
            }
            firstRecord = false
        }
        if(fieldCount != record.length){
            final String message = "Expected " + fieldCount + " fields but encountered " + record.length + " on row " + rowCounter
            throw new MalformedRecordException(message)
        }
        
        final Map<String, Object> recordMap = new HashMap<>()
        
        for (int i = 0; i < record.length; i++) {
            recordMap.put(headerNames.get(i).trim().toLowerCase(), record[i])
        }
        recordMap.put("wh_file_date",variables.get(CsvAttribute.FILE_DATE.getAttribute()))
        recordMap.put("wh_file_id",variables.get(CsvAttribute.FILE_ID.getAttribute()))
        recordMap.put("wh_row_id",rowCounter)
        final MapRecord mapRecord = new MapRecord(schema,recordMap)
        return mapRecord
    }
    
    Record processRecord() {
        if(iterator.hasNext()){
            ++rowCounter
            Map<String, Object> record = iterator.next()
            if(firstRecord){
                if (!useHeaders && hasSchema) {
                    fieldCount = record.size()
                    if (record.size() > headerNames.size()) {
                        final String message = "File has more columns than schema."
                        throw new MalformedRecordException(message)
                    }
                } 
                else {
                    fieldCount = headerNames.size()
                }
                firstRecord = false
            }
            if(fieldCount != record.size()){
                final String message = "Expected " + fieldCount + " fields but encountered " + record.size() + " on row " + rowCounter
                throw new MalformedRecordException(message)
            }
            final Map<String, Object> schemaCasedMap = convertKeysToSameCase(record, this.schema)
            final Map<String, String> recordMap = new LinkedHashMap<>(schemaCasedMap)
                       
            recordMap.put("wh_file_date",variables.get(CsvAttribute.FILE_DATE.getAttribute()))
            recordMap.put("wh_file_id",variables.get(CsvAttribute.FILE_ID.getAttribute()))
            recordMap.put("wh_row_id",rowCounter)
            final MapRecord mapRecord = new MapRecord(schema,recordMap)
            return mapRecord
        }
        return null
    }
    
    Map<String, Object> convertKeysToSameCase(final Map<String, Object> map, final RecordSchema recordSchema) {
        final Map<String, Object> newMap = new LinkedHashMap<>()
        final List<String> schemaFields = recordSchema.getFieldNames()
        final Map<String, String> schemaFieldMap = new HashMap<>()
        for (String schemaField : schemaFields) {
            schemaFieldMap.put(schemaField, schemaField)
        }

        final Set<Map.Entry<String, Object>> entrySet = map.entrySet()
        for (Map.Entry<String, Object> entry : entrySet) {
            newMap.put(schemaFieldMap.get(entry.getKey().trim().toLowerCase()), entry.getValue())
        }

        return newMap
    }
    
    @Override
    Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws IOException, MalformedRecordException {
        try {
            if (null != delimiter) {
                if (delimiter.size() < 2) {
                    return processRecord()
                } else {
                    return processRecord_multiDelimiter()
                }
            } else {
                return processBufferedRecord()
            }
        } 
        catch (Exception ex) {
            logger.error(ex.getMessage())
            throw ex
        }
    }

    @Override
    RecordSchema getSchema() throws MalformedRecordException {
        return schema
    }

    @Override
    void close() throws IOException {
        if (null != this.iterator) {
            this.iterator.close()
        }
        if (null != this.buffReader) {
            this.buffReader.close()
        }
        if (null != this.inputStream) {
            this.inputStream.close();
        }
    }
    
    private RecordSchema getSchemaFromList(final List<String> schemaString){
        final List<RecordField> fields = new ArrayList<>()
        for(String field : schemaString){
            fields.add(new RecordField(field.trim().toLowerCase(), RecordFieldType.STRING.dataType,true))
        }
        fields.addAll(getMetaFields())
        return new SimpleRecordSchema(fields)
    }
    
    private RecordSchema getNoDelimiterSchema(){
        final List<RecordField> fields = new ArrayList<>()
        fields.add(new RecordField("row_data",RecordFieldType.STRING.dataType))
        fields.addAll(getMetaFields())
        return new SimpleRecordSchema(fields)
    }
    
    private List<RecordField> getMetaFields() {
        final List<RecordField> fields = new ArrayList<>()
        fields.add(new RecordField("wh_file_date",RecordFieldType.LONG.dataType))
        fields.add(new RecordField("wh_file_id",RecordFieldType.STRING.dataType))
        fields.add(new RecordField("wh_row_id",RecordFieldType.LONG.dataType))
        return fields
    }
    
    
    private List<String> splitStringToList(final String string, final String splitDelimiter) {
        final List<String> list = new ArrayList<>()
        for(String item : string.split(splitDelimiter)){
            list.add(item)
        }
            
        return list
    }
}

class CSVRecordReaderFactory extends AbstractControllerService implements RecordReaderFactory {


    RecordReader createRecordReader(final Map<String, String> variables,
        final InputStream inputStream,
        final long inputLength,
        final ComponentLog logger) throws MalformedRecordException, IOException, SchemaNotFoundException {
        return new CSVRecordReader(variables,inputStream,inputLength,logger)
    }
}

reader = new CSVRecordReaderFactory()