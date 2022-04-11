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


/** Implementation of parquet.thrift LogicalType types. */
public abstract class LogicalType {

  /** Available set of logical types. */
  public enum Type {
    UNDEFINED, // = 0,  // Not a real logical type
    STRING, // = 1,
    MAP,
    LIST,
    ENUM,
    DECIMAL,
    DATE,
    TIME,
    TIMESTAMP,
    INTERVAL,
    INT,
    NIL, // Thrift NullType: annotates data that is always null
    JSON,
    BSON,
    UUID,

    // Not a real logical type; should always be last element
    NONE
  }

  /** Time units for temporal types. */
  public enum TimeUnit {
    UNKNOWN("unknown"), // = 0,
    MILLIS("milliseconds"), //= 1
    MICROS("microseconds"),
    NANOS("nanoseconds");

    private final String displayName;

    TimeUnit(String displayName) {
      this.displayName = displayName;
    }

    String displayName() {
      return displayName;
    }
  }

  /** Describe compatibility behavior, for isCompatible() and toConvertedType().
   *
   * If compatibility == CUSTOM_COMPATIBILITY, child classes must provide implementations for these methods.
   *
   * This is instead of the traits approach used in the CPP implementation.
   */
  protected enum Compatability {
    SIMPLE_COMPATIBLE,
    INCOMPATIBLE,
    CUSTOM_COMPATIBILITY
  }

  /** Describe applicability behavior, for isApplicable().
   *
   * If applicability == CUSTOM_APPLICABILITY, child classes must provide implementations for these methods.
   *
   * This is instead of the traits approach used in the CPP implementation.
   */
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

    this(type, order, Compatability.CUSTOM_COMPATIBILITY, Applicability.CUSTOM_APPLICABILITY,
        ConvertedType.NONE, ParquetType.UNDEFINED, -1);
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
      ParquetType primitiveType) {

    this(type, order, compatability, applicability, ConvertedType.NONE, primitiveType, -1);
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
      if (convertedType == ConvertedType.NONE) {
        throw new ParquetException("Converted type is required for simple compatibility");
      }
    }

    if (applicability == Applicability.SIMPLE_APPLICABLE) {
      if (primitiveType == ParquetType.UNDEFINED) {
        throw new ParquetException("Primitive (Parquet) type is required for simple applicability");
      }
    }

    if (applicability == Applicability.TYPE_LENGTH_APPLICABLE) {
      if (primitiveType == ParquetType.UNDEFINED || primitiveLength < 0) {
        throw new ParquetException("Primitive (Parquet) type and length are required for type length applicability");
      }
    }
  }

  /** f Return the enumerated type of this logical type. */
  public LogicalType.Type type() {
    return type;
  }

  /** Return the appropriate sort order for this logical type. */
  public SortOrder sortOrder() {
    return order;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) { return true; }
    if (!(o instanceof LogicalType)) { return false; }

    LogicalType that = (LogicalType) o;

    if (primitiveLength != that.primitiveLength) { return false; }
    if (type != that.type) { return false; }
    if (order != that.order) { return false; }
    if (compatability != that.compatability) { return false; }
    if (applicability != that.applicability) { return false; }
    if (convertedType != that.convertedType) { return false; }
    return primitiveType == that.primitiveType;
  }

  @Override
  public int hashCode() {
    int result = type != null ? type.hashCode() : 0;
    result = 31 * result + (order != null ? order.hashCode() : 0);
    result = 31 * result + (compatability != null ? compatability.hashCode() : 0);
    result = 31 * result + (applicability != null ? applicability.hashCode() : 0);
    result = 31 * result + (convertedType != null ? convertedType.hashCode() : 0);
    result = 31 * result + (primitiveType != null ? primitiveType.hashCode() : 0);
    result = 31 * result + primitiveLength;
    return result;
  }

  /**
   * If possible, return a logical type equivalent to the given legacy converted type
   * (and decimal metadata if applicable).
   */
  public static LogicalType fromConvertedType(ConvertedType convertedType, DecimalMetadata convertedDecimalMetadata) {

    // Default for decimal metadata in CPP implementation is {false, -1, -1}, or use DecimalMetadata.reset();

    switch (convertedType) {
      
      case UTF8:
        return new StringLogicalType();
        
      case MAP_KEY_VALUE:
      case MAP:
        return new MapLogicalType();
        
      case LIST:
        return new ListLogicalType();
        
      case ENUM:
        return new EnumLogicalType();
        
      case DECIMAL:
        return new DecimalLogicalType(convertedDecimalMetadata.precision(), convertedDecimalMetadata.scale());
        
      case DATE:
        return new DateLogicalType();
        
      case TIME_MILLIS:
        return new TimeLogicalType(true, TimeUnit.MILLIS);
        
      case TIME_MICROS:
        return new TimeLogicalType(true, TimeUnit.MICROS);

      case TIMESTAMP_MILLIS:
        return new TimestampLogicalType(true, TimeUnit.MILLIS,
            /* isFromConvertedType = */ true,
            /* forceSetConvertedType = */ false);
        
      case TIMESTAMP_MICROS:
        return new TimestampLogicalType(true, TimeUnit.MICROS,
            /* isFromConvertedType = */ true,
            /* forceSetConvertedType = */ false);
        
      case INTERVAL:
        return new IntervalLogicalType();
        
      case INT_8:
        return new IntLogicalType(8, true);
      case INT_16:
        return new IntLogicalType(16, true);
      case INT_32:
        return new IntLogicalType(32, true);
      case INT_64:
        return new IntLogicalType(64, true);
      case UINT_8:
        return new IntLogicalType(8, false);
      case UINT_16:
        return new IntLogicalType(16, false);
      case UINT_32:
        return new IntLogicalType(32, false);
      case UINT_64:
        return new IntLogicalType(64, false);
      case JSON:
        return new JsonLogicalType();
      case BSON:
        return new BsonLogicalType();
      case NA:
        return new NullLogicalType();
      case NONE:
        return new NoLogicalType();

      case UNDEFINED:
      default:
        return new UndefinedLogicalType();
    }
  }

  // TODO: Thrift
  // CPP type is format::LogicalType, in parquet_types.h under src/generated

  // /** Return the logical type represented by the Thrift intermediary object. */
  // public static LogicalType fromThrift(ThriftLogicalType thrift_logical_type) {
  //
  // }
  
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

    return new DecimalLogicalType(precision, scale);
  }

  public static LogicalType date() {

    return new DateLogicalType();
  }

  public static LogicalType time(boolean isAdjustedToUtc, TimeUnit timeUnit) {

    return new TimeLogicalType(isAdjustedToUtc, timeUnit);
  }

  /**
    Create a Timestamp logical type.

    @param isAdjustedToUtc set true if the data is UTC-normalized
    @param timeUnit the resolution of the timestamp
    @param isFromConvertedType
    if true, the timestamp was generated by translating a legacy converted type of
    TIMESTAMP_MILLIS or TIMESTAMP_MICROS. Default is false.
    @param forceSetConvertedType
    if true, always set the legacy ConvertedType TIMESTAMP_MICROS and TIMESTAMP_MILLIS metadata.
    Default is false

   */
  public static LogicalType timestamp(
      boolean isAdjustedToUtc, TimeUnit timeUnit,
      boolean isFromConvertedType /* = false */, boolean forceSetConvertedType /* = false */) {

    return new TimestampLogicalType(isAdjustedToUtc, timeUnit, isFromConvertedType, forceSetConvertedType);
  }

  public static LogicalType interval() {

    return new IntervalLogicalType();
  }

  public static LogicalType int_(int bitWidth, boolean isSigned) {
    return new IntLogicalType(bitWidth, isSigned);
  }

  /**
   * Create a logical type for data that's always null
   *
   * Any physical type can be annotated with this logical type.
   */
  public static LogicalType null_() {
    return new NullLogicalType();
  }

  public static LogicalType json() {
    return new JsonLogicalType();
  }

  public static LogicalType bson() {
    return new BsonLogicalType();
  }

  public static LogicalType uuid() {
    return new UuidLogicalType();
  }

  /** Create a placeholder for when no logical type is specified. */
  public static LogicalType none() {
    return new NoLogicalType();
  }

  /** Return true if this logical type is consistent with the given underlying physical type. */
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

  /**
   * Return true if this logical type is equivalent to the given legacy converted type
   * (and decimal metadata if applicable).
   */
  public boolean isCompatible(ConvertedType convertedType, DecimalMetadata convertedDecimalMetadata) {

    // default decimal metadata is: false, -1, -1

    switch (compatability) {

      case SIMPLE_COMPATIBLE:
        return (convertedType == this.convertedType) && !convertedDecimalMetadata.isSet();

      case INCOMPATIBLE:
        return (convertedType == ConvertedType.NONE || convertedType == ConvertedType.NA) &&
            !convertedDecimalMetadata.isSet();

      default:
        // Should never happen
        throw new ParquetException("Logical type compatibility is not specified");
    }
  }

  /**
   * If possible, return the legacy converted type (and decimal metadata if applicable)equivalent to this logical type.
   */
  public ConvertedType toConvertedType(DecimalMetadata outDecimalMetadata) {

    outDecimalMetadata.reset();

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

  /** Return a printable representation of this logical type. */
  @Override
  public abstract String toString();

  /** Return a JSON representation of this logical type. */
  public String toJson() {

    return "{\"Type\": \"" +
        this +
        "}";

  }

  // TODO: Thrift

  /// \brief Return a serializable Thrift object for this logical type.
  //    public Format.LogicalType toThrift() {
  //
  //    }

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

  public boolean isJson() {
    return type == Type.JSON;
  }

  public boolean isBson() {
    return type == Type.BSON;
  }

  public boolean isUuid() {
    return type == Type.UUID;
  }

  public boolean isNone() {
    return type == Type.NONE;
  }

  /** Return true if this logical type is of a known type. */
  public boolean isValid() {
    return type != Type.UNDEFINED;
  }

  public boolean isInvalid() {
    return !isValid();
  }

  /** Return true if this logical type is suitable for a schema GroupNode. */
  public boolean isNested() {
    return type == Type.LIST || type == Type.MAP;
  }

  public boolean isNonNested() {
    return !isNested();
  }

  public boolean isSerialized() {

    return !(type == Type.NONE || type == Type.UNDEFINED);
  }
}
