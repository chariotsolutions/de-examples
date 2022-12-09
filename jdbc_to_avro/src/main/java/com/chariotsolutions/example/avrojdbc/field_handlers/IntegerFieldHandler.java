package com.chariotsolutions.example.avrojdbc.field_handlers;

import org.apache.avro.Schema;


public class IntegerFieldHandler
extends FieldHandler
{
    public IntegerFieldHandler(String fieldName, int resultsetIndex, boolean isNullable)
    {
        super(fieldName, resultsetIndex, Schema.create(Schema.Type.INT), isNullable);
    }


    @Override
    public Object transform(Object value)
    {
        return value;
    }
}
