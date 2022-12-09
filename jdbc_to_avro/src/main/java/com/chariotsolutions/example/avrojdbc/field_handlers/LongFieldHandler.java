package com.chariotsolutions.example.avrojdbc.field_handlers;

import org.apache.avro.Schema;


public class LongFieldHandler
extends FieldHandler
{
    public LongFieldHandler(String fieldName, int resultsetIndex, boolean isNullable)
    {
        super(fieldName, resultsetIndex, Schema.create(Schema.Type.LONG), isNullable);
    }


    @Override
    public Object transform(Object value)
    {
        return value;
    }

}
