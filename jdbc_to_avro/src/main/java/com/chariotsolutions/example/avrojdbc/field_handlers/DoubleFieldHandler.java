package com.chariotsolutions.example.avrojdbc.field_handlers;

import org.apache.avro.Schema;


public class DoubleFieldHandler
extends FieldHandler
{
    public DoubleFieldHandler(String fieldName, int resultsetIndex, boolean isNullable)
    {
        super(fieldName, resultsetIndex, Schema.create(Schema.Type.DOUBLE), isNullable);
    }


    @Override
    public Object transform(Object value)
    {
        return value;
    }
}
