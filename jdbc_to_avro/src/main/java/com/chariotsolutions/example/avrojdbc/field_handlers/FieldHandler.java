package com.chariotsolutions.example.avrojdbc.field_handlers;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 *  Instances of this class retrieve a single field from the result-set,
 *  transform it, and store it in an Avro record.
 */
public abstract class FieldHandler
{
    protected String    fieldName;
    protected int       resultsetIndex;
    protected Schema    schema;


    protected FieldHandler(String fieldName, int resultsetIndex, Schema schema, boolean isNullable)
    {
        this.fieldName = fieldName;
        this.resultsetIndex = resultsetIndex;
        this.schema = isNullable
                    ? Schema.createUnion(schema,Schema.create(Schema.Type.NULL))
                    : schema;
    }

//----------------------------------------------------------------------------
//  Public methods
//----------------------------------------------------------------------------

    /**
     *  Returns a reference to this field for a record schema.
     */
    public Schema.Field asSchemaField()
    {
        return new Schema.Field(fieldName, schema);
    }


    /**
     *  Retrieves the field's value from a result-set and stores it in an Avro
     *  record.
     */
    public void store(ResultSet rslt, GenericRecord record)
    throws SQLException
    {
        record.put(fieldName, transform(rslt.getObject(resultsetIndex)));
    }

//----------------------------------------------------------------------------
//  Helper functions for subclasses
//----------------------------------------------------------------------------



//----------------------------------------------------------------------------
//  To be overridden by subclasses
//----------------------------------------------------------------------------

    /**
     *  Transforms the value into a form that can be stored in an Avro record.
     *
     *  @throws ClassCastException if the provided value is not compatible with
     *          the field handler.
     */
    public abstract Object transform(Object value);
}
