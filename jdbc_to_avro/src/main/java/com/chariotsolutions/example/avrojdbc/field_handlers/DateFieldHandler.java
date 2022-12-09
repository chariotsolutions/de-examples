package com.chariotsolutions.example.avrojdbc.field_handlers;

import java.sql.Date;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;


public class DateFieldHandler
extends FieldHandler
{
    public DateFieldHandler(String fieldName, int resultsetIndex, boolean isNullable)
    {
        super(fieldName, resultsetIndex,  mySchema(), isNullable);
    }


    @Override
    public Object transform(Object value)
    {
        if (value == null)
            return value;

        long millis = ((Date)value).getTime();
        int days = (int)(millis / 86400000);
        return Integer.valueOf(days);
    }


    private static Schema mySchema()
    {
        // note: this does not set the logical type field in the schema!
        return SchemaBuilder.builder().intBuilder()
                            .prop("logicalType", "date")
                            .endInt();
    }
}
