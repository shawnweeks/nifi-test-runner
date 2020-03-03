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
import org.apache.commons.csv.QuoteMode

class CSVRecordReader implements RecordReader {
    Map<String, String> variables
    InputStream inputStream
    long inputLength
    ComponentLog logger

    private RecordSchema schema

    private Reader reader
    private BufferedReader buffReader
    private Iterator<Object> iterator
    private long rowCounter
    private boolean firstRecord
    private int fieldCount
    private String delimiter
    private List<String> headerNames
    boolean hasSchema
    boolean useHeaders 

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

        hasSchema = variables.containsKey("wh_txt_schema")
        useHeaders = variables.containsKey("wh_txt_use_header")
  
        delimiter = variables.get("wh_txt_delim")
        String quote = variables.get("wh_txt_quote")
        String escape = variables.get("wh_txt_escape")
        int skipLines = variables.containsKey("wh_txt_skip_lines") ? Integer.parseInt(variables.get("wh_txt_skip_lines")) : 0
        
        reader = new InputStreamReader(inputStream,'UTF-8')
        buffReader = new BufferedReader(reader);
        for (int i = 0; i < skipLines; i++) {
            System.out.println( buffReader.readLine())
        }
        
        if (null != delimiter) {
            if (delimiter.length() < 2) {
                CSVFormat format = CSVFormat.newFormat(delimiter as char)
                if (null != quote) {
                    format = format.withQuote(quote as char)
                }
        
                if (null != escape) {
                    format = format.withEscape(escape as char)
                }
        
                if (useHeaders && !hasSchema) {
                    format = format.withFirstRecordAsHeader()
                }
                else if (!useHeaders && hasSchema) {
                    this.schema = getSchemaFromDelimitedString(variables.get("wh_txt_schema"))
                    this.headerNames = new ArrayList<>();
                    
                    for (String item : variables.get("wh_txt_schema").split(',')) {
                        this.headerNames.add(item);
                    }
                    format = format.withHeader(variables.get("wh_txt_schema").split(','));
                } 
                else if (useHeaders && hasSchema) {
                    this.schema = getSchemaFromDelimitedString(variables.get("wh_txt_schema"))
            
                    format = format.withFirstRecordAsHeader()
                }
                else {
                    logger.error("Useheader or schema must be provided.")
                    throw new MalformedRecordException("Useheader or schema must be provided.")
                }
   
                CSVParser parse = CSVParser.parse(buffReader, format);
                if (useHeaders && !hasSchema) {
                    this.headerNames = parse.getHeaderNames();
                    this.schema = getSchemaFromList(headerNames)
                } 
                else if (useHeaders && hasSchema) {
                    
                    this.headerNames = parse.getHeaderNames();
                    checkSchema(variables.get("wh_txt_schema").split(','), headerNames);
                }
             
                iterator = parse.iterator();
            } else {
                // Multi-char delimiter.
                if (useHeaders && !hasSchema) {
                    final String line = buffReader.readLine();
                    
                    headerNames = new ArrayList<>();
                    for (String item : line.split(delimiter)) {
                        headerNames.add(item);
                    }
                    this.schema = getSchemaFromDelimitedString(line, delimiter);
                }
                else if (!useHeaders && hasSchema) {
                    this.headerNames = new ArrayList<>();
                    for (String item : variables.get("wh_txt_schema").split(",")) {
                        headerNames.add(item);
                    }
                    this.schema = getSchemaFromDelimitedString(variables.get("wh_txt_schema"))
                } 
                else if (useHeaders && hasSchema) {
                    // Get headers in NextRecord function.
                    final String[] line = buffReader.readLine().split(delimiter);
                     
                    headerNames = new ArrayList<>();
                    for (String item : line) {
                        headerNames.add(item);
                    }
                    checkSchema(variables.get("wh_txt_schema").split(','), headerNames);
                    this.schema = getSchemaFromDelimitedString(variables.get("wh_txt_schema"))
                }
                else {
                    logger.error("Useheader or schema must be provided.")
                    throw new MalformedRecordException("Useheader or schema must be provided.")
                }
            }
        } else {
            this.schema = getNoDelimiterSchema()
        }

        rowCounter = 0
        firstRecord = true
    }
        
    public void checkSchema(String[] recordSchema, List<String> colSubset) {
        if (recordSchema.length < colSubset.size()) {
            logger.error("File has more columns than schema.")
            throw new MalformedRecordException("File has more columns than schema.");
        }

        for (String col : colSubset) {
            boolean found = false;
            for (String schemaCol : recordSchema) {
                if (col.equals(schemaCol)) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                logger.error("Column '" + col + "' not in schema.")
                throw new MalformedRecordException("Column '" + col + "' not in schema.");
            }
        }
    }
    
    Record processBufferedRecord() {
        String line = buffReader.readLine()
        if (null == line) {
            return null
        }
        ++rowCounter
        Map<String, Object> recordMap = new HashMap<>()
        recordMap.put("row_data", line)
        recordMap.put("wh_file_date",variables.get("wh_file_date"))
        recordMap.put("wh_file_id",variables.get("wh_file_id"))
        recordMap.put("wh_row_id",rowCounter)
        MapRecord mapRecord = new MapRecord(schema,recordMap)
        return mapRecord
    }
    
    Record processRecord_multiDelimiter() {
        String line = buffReader.readLine()
        if (null == line) {
            return null
        }
        ++rowCounter
        final String[] lineSplit = line.split(delimiter);
        if(firstRecord){
            if (!useHeaders && hasSchema) {
                fieldCount = lineSplit.size();
                System.out.println("lineSplit: " + lineSplit.size())
                System.out.println("headerNames: " + headerNames.size())
                if (lineSplit.size() > headerNames.size()) {
                    System.out.println("WTF")
                    logger.error("File has more columns than schema.")
                    throw new MalformedRecordException("File has more columns than schema.");
                }
            } 
            else if (useHeaders && !hasSchema) {
                fieldCount = headerNames.size();
            }
            else {
                fieldCount = headerNames.size()
            }
            firstRecord = false
        }
        if(fieldCount != lineSplit.length){
            logger.error("Expected " + fieldCount + " fields but encountered " + lineSplit.length + " on row " + rowCounter)
            throw new MalformedRecordException("Expected " + fieldCount + " fields but encountered " + lineSplit.length + " on row " + rowCounter)
        }
        Map<String, Object> recordMap = new HashMap<>()
        
        for (int i = 0; i < lineSplit.length; i++) {
            recordMap.put(headerNames.get(i), lineSplit[i]);
        }
        recordMap.put("wh_file_date",variables.get("wh_file_date"))
        recordMap.put("wh_file_id",variables.get("wh_file_id"))
        recordMap.put("wh_row_id",rowCounter)
        MapRecord mapRecord = new MapRecord(schema,recordMap)
        return mapRecord
    }

    Record processRecord() {
        if(iterator.hasNext()){
            ++rowCounter
            CSVRecord record = iterator.next() as CSVRecord
            if(firstRecord){
                if (!useHeaders && hasSchema) {
                    fieldCount = record.size();
                    System.out.println("record: " + record.size())
                    System.out.println("headerNames: " + headerNames.size())
                    if (record.size() > headerNames.size()) {
                        logger.error("File has more columns than schema.")
                        throw new MalformedRecordException("File has more columns than schema.");
                    }
                } 
                else if (useHeaders && !hasSchema) {
                    fieldCount = headerNames.size();
                }
                else {
                    fieldCount = headerNames.size()
                }
                firstRecord = false
            }
            if(fieldCount != record.size()){
                logger.error("Expected " + fieldCount + " fields but encountered " + record.size() + " on row " + rowCounter)
                throw new MalformedRecordException("Expected " + fieldCount + " fields but encountered " + record.size() + " on row " + rowCounter)
            }
            Map<String, Object> recordMap = record.toMap()
                       
            recordMap.put("wh_file_date",variables.get("wh_file_date"))
            recordMap.put("wh_file_id",variables.get("wh_file_id"))
            recordMap.put("wh_row_id",rowCounter)
            MapRecord mapRecord = new MapRecord(schema,recordMap)
            return mapRecord
        }
        return null
    }
    
    @Override
    Record nextRecord(boolean b, boolean b1) throws IOException, MalformedRecordException {
        try {
            if (null != delimiter) {
                if (delimiter.size() < 2) {
                    return processRecord();
                } else {
                    return processRecord_multiDelimiter();
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
        reader.close()
    }
    
    private RecordSchema getSchemaFromList(List<String> schemaString){
        List<RecordField> fields = new ArrayList<>()
        for(String field : schemaString){
            fields.add(new RecordField(field, RecordFieldType.STRING.dataType,true))
        }
        fields.add(new RecordField("wh_file_date",RecordFieldType.LONG.dataType))
        fields.add(new RecordField("wh_file_id",RecordFieldType.STRING.dataType))
        fields.add(new RecordField("wh_row_id",RecordFieldType.TIMESTAMP.dataType))
        return new SimpleRecordSchema(fields)
    }
    
    private RecordSchema getNoDelimiterSchema(){
        List<RecordField> fields = new ArrayList<>()
        fields.add(new RecordField("row_data",RecordFieldType.STRING.dataType))
        fields.add(new RecordField("wh_file_date",RecordFieldType.LONG.dataType))
        fields.add(new RecordField("wh_file_id",RecordFieldType.STRING.dataType))
        fields.add(new RecordField("wh_row_id",RecordFieldType.TIMESTAMP.dataType))
        return new SimpleRecordSchema(fields)
    }

    private RecordSchema getSchemaFromDelimitedString(String schemaString){
        return getSchemaFromDelimitedString(schemaString, ",")
    }
    
    private RecordSchema getSchemaFromDelimitedString(String schemaString, String splitDelimiter){
        List<RecordField> fields = new ArrayList<>()
        for(String field : schemaString.split(splitDelimiter)){
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