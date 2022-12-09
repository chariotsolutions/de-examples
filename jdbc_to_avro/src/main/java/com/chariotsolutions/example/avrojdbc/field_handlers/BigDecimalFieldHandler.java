package com.chariotsolutions.example.avrojdbc.field_handlers;

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.Conversions.DecimalConversion;


public class BigDecimalFieldHandler
extends FieldHandler
{
    private final static DecimalConversion DECIMAL_CONVERTER = new DecimalConversion();

    private LogicalType logicalType;
    private int scale;


    public BigDecimalFieldHandler(String fieldName, int resultsetIndex, int precision, int scale, boolean isNullable)
    {
        super(fieldName, resultsetIndex, mySchema(precision, scale), isNullable);
        this.scale = scale;
        this.logicalType = LogicalTypes.decimal(precision, scale);
    }


    @Override
    public Object transform(Object value)
    {
        if (value == null)
            return value;

        // note: since we got scale from the ResultSet, there should be no need to round the actual value
        BigDecimal dval = ((BigDecimal)value).setScale(scale, RoundingMode.HALF_EVEN);
        return DECIMAL_CONVERTER.toBytes(dval, schema, logicalType);
    }


    private static Schema mySchema(int precision, int scale)
    {
        // note: this does not set the logical type field in the schema!
        return SchemaBuilder.builder().bytesBuilder()
                            .prop("logicalType", "decimal")
                            .prop("precision", precision)
                            .prop("scale", scale)
                            .endBytes();
    }
}
