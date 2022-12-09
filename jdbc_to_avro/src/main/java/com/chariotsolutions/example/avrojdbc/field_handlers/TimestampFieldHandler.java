package com.chariotsolutions.example.avrojdbc.field_handlers;

import java.sql.Timestamp;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;


public class TimestampFieldHandler
extends FieldHandler
{
    public TimestampFieldHandler(String fieldName, int resultsetIndex, boolean isNullable)
    {
        super(fieldName, resultsetIndex, mySchema(), isNullable);
    }


    @Override
    public Object transform(Object value)
    {
        return value == null
             ? value
             : Long.valueOf(((Timestamp)value).getTime());
    }


    private static Schema mySchema()
    {
        // note: this does not set the logical type field in the schema!
        return SchemaBuilder.builder().longBuilder()
                            .prop("logicalType", "timestamp-millis")
                            .endLong();
    }

}
