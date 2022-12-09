package com.chariotsolutions.example.avrojdbc;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chariotsolutions.example.avrojdbc.field_handlers.*;


/**
 *  Writes a JDBC <code>ResultSet</code> as an Avro file, building the schema
 *  from the <code>ResultSetMetaData</code>. Currently supports standard scalar
 *  types.
 */
public class ResultSetHandler
{
    private static Logger logger = LoggerFactory.getLogger(ResultSetHandler.class);

    /**
     *  Reads the provided ResultSet and writes it to a file.
     */
    public static void writeAvro(String namespace, String recordName, ResultSet rslt, File outputFile)
    throws IOException, SQLException
    {
        List<FieldHandler> fieldHandlers = extractFields(rslt.getMetaData());
        Schema schema = buildSchema(namespace, recordName, fieldHandlers);

        // delete output file because otherwise the Avro writer will complain
        outputFile.delete();

        int rowcount = 0;
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(datumWriter))
        {
            writer.create(schema, outputFile);
            while (rslt.next())
            {
                GenericRecord datum = new GenericData.Record(schema);
                for (FieldHandler handler : fieldHandlers)
                {
                    handler.store(rslt, datum);
                }
                writer.append(datum);
                rowcount++;
            }
        }
        logger.info("wrote {} rows", rowcount);
    }


    /**
     *  Creates a field handler for each of the fields in the result-set metadata.
     *  Note: ignores any fields where we don't support the field type.
     */
    private static List<FieldHandler> extractFields(ResultSetMetaData metadata)
    throws SQLException
    {
        List<FieldHandler> handlers = new ArrayList<>();
        for (int ii = 1 ; ii <= metadata.getColumnCount() ; ii++)
        {
            String columnName = metadata.getColumnName(ii);
            String columnClass = metadata.getColumnClassName(ii);
            boolean isNullable = metadata.isNullable(ii) != ResultSetMetaData.columnNoNulls;

            switch (columnClass)
            {
                case "java.lang.String":
                    handlers.add(new StringFieldHandler(columnName, ii, isNullable));
                    break;
                case "java.lang.Integer":
                    handlers.add(new IntegerFieldHandler(columnName, ii, isNullable));
                    break;
                case "java.lang.Long":
                    handlers.add(new LongFieldHandler(columnName, ii, isNullable));
                    break;
                case "java.lang.Float":
                    handlers.add(new FloatFieldHandler(columnName, ii, isNullable));
                    break;
                case "java.lang.Double":
                    handlers.add(new DoubleFieldHandler(columnName, ii, isNullable));
                    break;
                case "java.math.BigDecimal":
                    handlers.add(new BigDecimalFieldHandler(columnName, ii, metadata.getPrecision(ii), metadata.getScale(ii), isNullable));
                    break;
                case "java.sql.Date":
                    handlers.add(new DateFieldHandler(columnName, ii, isNullable));
                    break;
                case "java.sql.Timestamp":
                    handlers.add(new TimestampFieldHandler(columnName, ii, isNullable));
                    break;
                default:
                    throw new IllegalArgumentException("column \"" + columnName + "\" has unsupported type: " + columnClass);
            }
        }
        return handlers;
    }


    /**
     *  Builds the Avro schema from the list of supported fields.
     */
    private static Schema buildSchema(String namespace, String recordName, List<FieldHandler> fieldHandlers)
    {
        logger.debug("creating schema from {} field handlers", fieldHandlers.size());
        List<Schema.Field> fields = fieldHandlers.stream().map(FieldHandler::asSchemaField).collect(Collectors.toList());
        Schema schema = Schema.createRecord(recordName, "", namespace, false, fields);
        logger.debug("schema: {}", schema);
        return schema;
    }
}
