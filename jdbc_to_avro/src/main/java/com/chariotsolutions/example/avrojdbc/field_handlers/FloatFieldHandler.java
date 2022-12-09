package com.chariotsolutions.example.avrojdbc.field_handlers;

import org.apache.avro.Schema;


public class FloatFieldHandler
extends FieldHandler
{
    public FloatFieldHandler(String fieldName, int resultsetIndex, boolean isNullable)
    {
        super(fieldName, resultsetIndex, Schema.create(Schema.Type.FLOAT), isNullable);
    }


    @Override
    public Object transform(Object value)
    {
        return value;
    }
}
