/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.adapter.parquet.type;


import org.apache.arrow.adapter.parquet.ParquetException;

public abstract class LogicalType {

    public enum Type {
        UNDEFINED,  // = 0,  // Not a real logical type
        STRING,  // = 1,
        MAP,
        LIST,
        ENUM,
        DECIMAL,
        DATE,
        TIME,
        TIMESTAMP,
        INTERVAL,
        INT,
        NIL,  // Thrift NullType: annotates data that is always null
        JSON,
        BSON,
        UUID,

        // Not a real logical type; should always be last element
        NONE
    }

    public enum TimeUnit {
        UNKNOWN, // = 0,
        MILLIS,  //= 1
        MICROS,
        NANOS
    }

    protected enum Compatability {
        SIMPLE_COMPATIBLE,
        INCOMPATIBLE,
        CUSTOM_COMPATIBILITY
    }

    protected enum Applicability {
        SIMPLE_APPLICABLE,
        TYPE_LENGTH_APPLICABLE,
        UNIVERSAL_APPLICABLE,
        INAPPLICABLE,
        CUSTOM_APPLICABILITY
    }


    private final Type type;
    private final SortOrder order;

    private final Compatability compatability;
    private final Applicability applicability;
    private final ConvertedType convertedType;
    private final ParquetType primitiveType;
    private final int primitiveLength;


    protected LogicalType(Type type, SortOrder order) {

        this(type, order, Compatability.CUSTOM_COMPATIBILITY, Applicability.CUSTOM_APPLICABILITY, ConvertedType.NONE, ParquetType.UNDEFINED, -1);
    }

    protected LogicalType(
            Type type, SortOrder order,
            Compatability compatability, Applicability applicability) {

        this(type, order, compatability, applicability, ConvertedType.NONE, ParquetType.UNDEFINED, -1);
    }

    protected LogicalType(
            Type type, SortOrder order,
            Compatability compatability, Applicability applicability,
            ConvertedType convertedType) {

        this(type, order, compatability, applicability, convertedType, ParquetType.UNDEFINED, -1);
    }

    protected LogicalType(
            Type type, SortOrder order,
            Compatability compatability, Applicability applicability,
            ConvertedType convertedType, ParquetType primitiveType) {

        this(type, order, compatability, applicability, convertedType, primitiveType, -1);
    }

    protected LogicalType(
            Type type, SortOrder order,
            Compatability compatability, Applicability applicability,
             ParquetType primitiveType, int primitiveLength) {

        this(type, order, compatability, applicability, ConvertedType.NONE, primitiveType, primitiveLength);
    }

    protected LogicalType(
            Type type, SortOrder order,
            Compatability compatability, Applicability applicability,
            ConvertedType convertedType, ParquetType primitiveType, int primitiveLength) {

        this.type = type;
        this.order = order;

        this.compatability = compatability;
        this.applicability = applicability;
        this.convertedType = convertedType;
        this.primitiveType = primitiveType;
        this.primitiveLength = primitiveLength;

        if (compatability == Compatability.SIMPLE_COMPATIBLE) {
            if (convertedType == ConvertedType.NONE)
                throw new ParquetException("Converted type is required for simple compatibility");
        }

        if (applicability == Applicability.SIMPLE_APPLICABLE) {
            if (primitiveType == ParquetType.UNDEFINED)
                throw new ParquetException("Primitive (Parquet) type is required for simple applicability");
        }

        if (applicability == Applicability.TYPE_LENGTH_APPLICABLE) {
            if (primitiveType == ParquetType.UNDEFINED || primitiveLength < 0)
                throw new ParquetException("Primitive (Parquet) type and length are required for type length applicability");
        }

    }

//    public static LogicalType fromConvertedType(
//      ConvertedType convertedType,
//      const parquet::schema::DecimalMetadata converted_decimal_metadata = {false, -1,
//            -1});

//    /// \brief Return the logical type represented by the Thrift intermediary object.
//    public static LogicalType fromThrift(
//      const parquet::format::LogicalType& thrift_logical_type);


    /// \brief Return the explicitly requested logical type.

    public static LogicalType string() {
        return new StringLogicalType();
    }

    public static LogicalType map() {
        return new MapLogicalType();
    }

    public static LogicalType list() {
        return new ListLogicalType();
    }

    public static LogicalType enum_() {
        return new EnumLogicalType();
    }

    public static LogicalType decimal(int precision, int scale /*= 0 */) {

        return null;  // todo
    }

    public static LogicalType date() {

        return null;  // todo
    }

    public static LogicalType time(boolean isAdjustedToUtc, TimeUnit timeUnit) {

        return null;  // todo
    }



    /// \brief Create a Timestamp logical type
    /// \param[in] is_adjusted_to_utc set true if the data is UTC-normalized
    /// \param[in] time_unit the resolution of the timestamp
    /// \param[in] is_from_converted_type if true, the timestamp was generated
    /// by translating a legacy converted type of TIMESTAMP_MILLIS or
    /// TIMESTAMP_MICROS. Default is false.
    /// \param[in] force_set_converted_type if true, always set the
    /// legacy ConvertedType TIMESTAMP_MICROS and TIMESTAMP_MILLIS
    /// metadata. Default is false
    public static LogicalType timestamp(
            boolean isAdjustedToUtc, TimeUnit timeUnit,
            boolean isFromConvertedType /* = false */, boolean forceSetConvertedType /* = false */) {

        return null;  // todo
    }

    public static LogicalType interval() {

        return null;  // todo
    }

    public static LogicalType int_(int bitWidth, boolean isSigned) {
        return new IntLogicalType(bitWidth, isSigned);
    }

    /// \brief Create a logical type for data that's always null
    ///
    /// Any physical type can be annotated with this logical type.
    public static LogicalType null_() {
        return new NullLogicalType();
    }

    public static LogicalType JSON() {
        return new JsonLogicalType();
    }

    public static LogicalType BSON() {
        return new BsonLogicalType();
    }

    public static LogicalType UUID() {
        return new UuidLogicalType();
    }

    /// \brief Create a placeholder for when no logical type is specified
    public static LogicalType none() {
        return new NoLogicalType();
    }

    /// \brief Return true if this logical type is consistent with the given underlying
    /// physical type.
    public boolean isApplicable(ParquetType primitiveType, int primitiveLength /*= -1 */) {

        switch (applicability) {

            case SIMPLE_APPLICABLE:
                return primitiveType == this.primitiveType;

            case TYPE_LENGTH_APPLICABLE:
                return primitiveType == this.primitiveType && primitiveLength == this.primitiveLength;

            case UNIVERSAL_APPLICABLE:
                return true;

            case INAPPLICABLE:
                return false;

            default:
                // Should never happen
                throw new ParquetException("Logical type applicability is not specified");
        }
    }

    /// \brief Return true if this logical type is equivalent to the given legacy converted
    /// type (and decimal metadata if applicable).
    public boolean isCompatible(ConvertedType convertedType, DecimalMetadata convertedDecimalMetadata) {

        // default decimal metadata is: false, -1, -1

        switch (compatability) {

            case SIMPLE_COMPATIBLE:
                return (convertedType == this.convertedType) && !convertedDecimalMetadata.isSet();

            case INCOMPATIBLE:
                return (convertedType == ConvertedType.NONE || convertedType == ConvertedType.NA)
                        && !convertedDecimalMetadata.isSet();

            default:
                // Should never happen
                throw new ParquetException("Logical type compatibility is not specified");
        }
    }

    /// \brief If possible, return the legacy converted type (and decimal metadata if
    /// applicable) equivalent to this logical type.
    public ConvertedType toConvertedType(DecimalMetadata outDecimalMetadata) {

        outDecimalMetadata.reset();;

        switch (compatability) {

            case SIMPLE_COMPATIBLE:
                return convertedType;

            case INCOMPATIBLE:
                return ConvertedType.NONE;

            default:
                // Should never happen
                throw new ParquetException("Logical type compatibility is not specified");
        }
    }

    /// \brief Return a printable representation of this logical type.
    public abstract String toString();

    /// \brief Return a JSON representation of this logical type.
    public String toJSON() {

        StringBuilder sb = new StringBuilder();
        sb.append("{\"Type\": \"");
        sb.append(this);
        sb.append("}");

        return sb.toString();

    }

    // TODO: Thrift

    /// \brief Return a serializable Thrift object for this logical type.
//    public Format.LogicalType toThrift() {
//
//    }

    /// \brief Return true if the given logical type is equivalent to this logical type.
    public boolean equals(LogicalType other) {
        return other.type == type;
    }

    /// \brief Return the enumerated type of this logical type.
    public LogicalType.Type type() {
        return type;
    }

    /// \brief Return the appropriate sort order for this logical type.
    public SortOrder sortOrder() {
        return order;
    }

    public boolean isString() {
        return type == Type.STRING;
    }

    public boolean isMap() {
        return type == Type.MAP;
    }

    public boolean isList() {
        return type == Type.LIST;
    }

    public boolean isEnum() {
        return type == Type.ENUM;
    }

    public boolean isDecimal() {
        return type == Type.DECIMAL;
    }

    public boolean isDate() {
        return type == Type.DATE;
    }

    public boolean isTime() {
        return type == Type.TIME;
    }

    public boolean isTimestamp() {
        return type == Type.TIMESTAMP;
    }

    public boolean isInterval() {
        return type == Type.INTERVAL;
    }

    public boolean isInt() {
        return type == Type.INT;
    }

    public boolean isNull() {
        return type == Type.NIL;
    }

    public boolean isJSON() {
        return type == Type.JSON;
    }

    public boolean isBSON() {
        return type == Type.BSON;
    }

    public boolean isUUID() {
        return type == Type.UUID;
    }

    public boolean isNone() {
        return type == Type.NONE;
    }

    /// \brief Return true if this logical type is of a known type.
    public boolean isValid() {
        return type != Type.UNDEFINED;
    }

    public boolean isInvalid() {
        return ! isValid();
    }

    /// \brief Return true if this logical type is suitable for a schema GroupNode.
    public boolean isNested() {
        return type == Type.LIST || type == Type.MAP;
    }

    public boolean isNonNested() {
        return ! isNested();
    }

    /// \brief Return true if this logical type is included in the Thrift output for its
    /// node.
    public boolean isSerialized() {

        return false;  // todo
    }
}
