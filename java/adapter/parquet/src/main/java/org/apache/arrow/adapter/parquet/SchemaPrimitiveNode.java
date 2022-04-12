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

package org.apache.arrow.adapter.parquet;

import org.apache.arrow.adapter.parquet.type.ConvertedType;
import org.apache.arrow.adapter.parquet.type.DecimalMetadata;
import org.apache.arrow.adapter.parquet.type.LogicalType;
import org.apache.arrow.adapter.parquet.type.NoLogicalType;
import org.apache.arrow.adapter.parquet.type.ParquetType;
import org.apache.arrow.adapter.parquet.type.RepetitionType;


/**
 * Schema node for a type that is one of the primitive Parquet storage types.
 *
 * In addition to the other type metadata (name, repetition level, logical type), also has the
 * physical storage type and their type-specific metadata (byte width, decimal parameters).
 */
public class SchemaPrimitiveNode extends SchemaNode {

  private final ParquetType physicalType;
  protected int typeLength;
  private final DecimalMetadata decimalMetadata;
  private ColumnOrder columnOrder;

  /** Create a schema node from an opaque schema element. */
  public static SchemaNode fromParquet(org.apache.parquet.format.SchemaElement schemaElement) {

    int fieldId = schemaElement.isSetField_id() ? schemaElement.getField_id() : -1;

    if (schemaElement.isSetLogicalType()) {

      // updated writer with logical type present

      return new SchemaPrimitiveNode(
          schemaElement.getName(),
          convertEnum(RepetitionType.class, schemaElement.getRepetition_type()),
          LogicalType.fromThrift(schemaElement.getLogicalType()),
          convertEnum(ParquetType.class, schemaElement.getType()),
          schemaElement.getType_length(),
          schemaElement.getField_id());
    }

    if (schemaElement.isSetConverted_type()) {

      // legacy writer with converted type present

      return new SchemaPrimitiveNode(
          schemaElement.getName(),
          convertEnum(RepetitionType.class, schemaElement.getRepetition_type()),
          convertEnum(ParquetType.class, schemaElement.getType()),
          convertEnum(ConvertedType.class, schemaElement.getConverted_type()),
          schemaElement.getType_length(), schemaElement.getPrecision(), schemaElement.getScale(), fieldId);
    }

    // logical type not present

    return new SchemaPrimitiveNode(
        schemaElement.getName(),
        convertEnum(RepetitionType.class, schemaElement.getRepetition_type()),
        new NoLogicalType(),
        convertEnum(ParquetType.class, schemaElement.getType()),
        schemaElement.getType_length(),
        fieldId);
  }

  private static <T extends Enum<T>, S extends Enum<S>> T convertEnum(Class<T> enumClass, S thriftEnum) {

    return Enum.valueOf(enumClass, thriftEnum.name());
  }

  /**
   * Create a schema node with the given type information.
   *
   * If no logical type, pass LogicalType::None() or null.
   * A fieldId -1 (or any negative value) will be serialized as null in Thrift.
   */
  public SchemaPrimitiveNode(
      String name, RepetitionType repetition,
      LogicalType logicalType, ParquetType physicalType) {

    this(name, repetition, logicalType, physicalType, /* physicalLength = */ -1, /* fieldId = */ -1);
  }

  /**
   * Create a schema node with the given type information.
   *
   * A fieldId -1 (or any negative value) will be serialized as null in Thrift.
   */
  public SchemaPrimitiveNode(
      String name, RepetitionType repetition,
      ParquetType physicalType) {

    this(name, repetition, physicalType, ConvertedType.NONE,
        /* length = */ -1, /* precision = */ -1, /* scale = */ -1, /* fieldId = */ -1);
  }

  /**
   * Create a schema node with the given type information.
   *
   * A fieldId -1 (or any negative value) will be serialized as null in Thrift.
   */
  public SchemaPrimitiveNode(
      String name, RepetitionType repetition,
      ParquetType physicalType, ConvertedType convertedType) {

    this(name, repetition, physicalType, convertedType,
        /* length = */ -1, /* precision = */ -1, /* scale = */ -1, /* fieldId = */ -1);
  }

  /**
   * Create a schema node with the given type information.
   *
   * A fieldId -1 (or any negative value) will be serialized as null in Thrift.
   */
  public SchemaPrimitiveNode(
      String name, RepetitionType repetition,
      ParquetType physicalType, ConvertedType convertedType, int length) {

    this(name, repetition, physicalType, convertedType,
        length, /* precision = */ -1, /* scale = */ -1, /* fieldId = */ -1);
  }

  /**
   * Create a schema node with the given type information.
   *
   * A fieldId -1 (or any negative value) will be serialized as null in Thrift.
   */
  public SchemaPrimitiveNode(
      String name, RepetitionType repetition,
      ParquetType physicalType, ConvertedType convertedType,
      int length, int precision, int scale) {

    this(name, repetition, physicalType, convertedType, length, precision, scale, /* fieldId = */ -1);
  }

  /**
   * Create a schema node with the given type information.
   *
   * A fieldId -1 (or any negative value) will be serialized as null in Thrift.
   */
  public SchemaPrimitiveNode(
      String name, RepetitionType repetition,
      ParquetType physicalType, ConvertedType convertedType,
      int length, int precision, int scale, int fieldId) {

    super(Type.PRIMITIVE, name, repetition, convertedType, fieldId);

    this.physicalType = physicalType;
    this.typeLength = length;

    // PARQUET-842: In an earlier revision, decimal_metadata_.isset was being
    // set to true, but Impala will raise an incompatible metadata in such cases
    this.decimalMetadata = DecimalMetadata.zeroUnset();

    // Check if the physical and logical types match
    // Mapping referred from Apache parquet-mr as on 2016-02-22
    switch (convertedType) {

      case NONE:
        // Logical type not set
        break;

      case UTF8:
      case JSON:
      case BSON:

        if (physicalType != ParquetType.BYTE_ARRAY) {
          throw new ParquetException(convertedType.name() + " can only annotate BYTE_ARRAY fields");
        }

        break;

      case DECIMAL:

        if ((physicalType != ParquetType.INT32) &&
            (physicalType != ParquetType.INT64) &&
            (physicalType != ParquetType.BYTE_ARRAY) &&
            (physicalType != ParquetType.FIXED_LEN_BYTE_ARRAY)) {
          throw new ParquetException("DECIMAL can only annotate INT32, INT64, BYTE_ARRAY, and FIXED");
        }

        if (precision <= 0) {
          throw new ParquetException(
              "Invalid DECIMAL precision: " + precision +
              ". Precision must be a number between 1 and 38 inclusive");
        }

        if (scale < 0) {
          throw new ParquetException(
              "Invalid DECIMAL scale: " + scale +
              ". Scale must be a number between 0 and precision inclusive");
        }

        if (scale > precision) {
          throw new ParquetException(
              "Invalid DECIMAL scale " + scale +
            " cannot be greater than precision " + precision);
        }

        decimalMetadata.set(true, precision, scale);

        break;

      case DATE:
      case TIME_MILLIS:
      case UINT_8:
      case UINT_16:
      case UINT_32:
      case INT_8:
      case INT_16:
      case INT_32:

        if (physicalType != ParquetType.INT32) {
          throw new ParquetException(convertedType.name() + " can only annotate INT32");
        }

        break;

      case TIME_MICROS:
      case TIMESTAMP_MILLIS:
      case TIMESTAMP_MICROS:
      case UINT_64:
      case INT_64:

        if (physicalType != ParquetType.INT64) {
          throw new ParquetException(convertedType.name() + " can only annotate INT64");
        }

        break;

      case INTERVAL:

        if ((physicalType != ParquetType.FIXED_LEN_BYTE_ARRAY) || (length != 12)) {
          throw new ParquetException("INTERVAL can only annotate FIXED_LEN_BYTE_ARRAY(12)");
        }

        break;

      case ENUM:

        if (physicalType != ParquetType.BYTE_ARRAY) {
          throw new ParquetException("ENUM can only annotate BYTE_ARRAY fields");
        }

        break;

      case NA:

        // NA can annotate any type
        break;

      default:

        throw new ParquetException(convertedType.name() + " cannot be applied to a primitive type");
    }

    // For forward compatibility, create an equivalent logical type
    this.logicalType = LogicalType.fromConvertedType(convertedType, decimalMetadata);

    checkAssignedTypes();
  }

  /**
   * Create a schema node with the given type information.
   *
   * If no logical type, pass LogicalType::None() or null.
   * A fieldId -1 (or any negative value) will be serialized as null in Thrift.
   */
  public SchemaPrimitiveNode(
      String name, RepetitionType repetition, LogicalType logicalType,
      ParquetType physicalType, int physicalLength, int fieldId) {

    super(Type.PRIMITIVE, name, repetition, logicalType, fieldId);

    this.physicalType = physicalType;
    this.typeLength = physicalLength;

    // PARQUET-842: In an earlier revision, decimal_metadata_.isset was being
    // set to true, but Impala will raise an incompatible metadata in such cases
    this.decimalMetadata = DecimalMetadata.zeroUnset();

    if (logicalType != null) {

      // Check for logical type <=> node type consistency

      if (logicalType.isNested()) {
        throw new ParquetException("Nested logical type " + logicalType + " can not be applied to non-group node");
      }

      // Check for logical type <=> physical type consistency
      if (!logicalType.isApplicable(physicalType, physicalLength)) {
        throw new ParquetException(logicalType + " can not be applied to primitive type " + physicalType.name());
      }

    } else {

      logicalType = new NoLogicalType();
    }

    convertedType = logicalType.toConvertedType(decimalMetadata);

    checkAssignedTypes();
  }

  private void checkAssignedTypes() {

    if (!(logicalType != null &&
        !logicalType.isNested() &&
        logicalType.isCompatible(convertedType, decimalMetadata))) {

      if (logicalType == null) {
        throw new ParquetException("Invalid logical type: (null)");
      } else {
        throw new ParquetException("Invalid logical type: " + logicalType);
      }
    }

    if (physicalType == ParquetType.FIXED_LEN_BYTE_ARRAY) {
      if (typeLength <= 0) {
        throw new ParquetException("Invalid FIXED_LEN_BYTE_ARRAY length: " + typeLength);
      }
    }
  }


  public ParquetType physicalType() {
    return physicalType;
  }

  public int typeLength() {
    return typeLength;
  }

  public DecimalMetadata decimalMetadata() {
    return decimalMetadata;
  }

  public ColumnOrder columnOrder() {
    return columnOrder;
  }

  public void setColumnOrder(ColumnOrder columnOrder) {
    this.columnOrder = columnOrder;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) { return true; }
    if (!(o instanceof SchemaPrimitiveNode)) { return false; }
    if (!super.equals(o)) { return false; }

    SchemaPrimitiveNode that = (SchemaPrimitiveNode) o;

    if (physicalType != that.physicalType) { return false; }

    if (convertedType == ConvertedType.DECIMAL) {
      if (!decimalMetadata.equals(that.decimalMetadata)) { return false; }
    }

    if (physicalType == ParquetType.FIXED_LEN_BYTE_ARRAY) {
      return typeLength == that.typeLength;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (physicalType != null ? physicalType.hashCode() : 0);
    if (convertedType == ConvertedType.DECIMAL) {
      result = 31 * result + (decimalMetadata != null ? decimalMetadata.hashCode() : 0);
    }
    if (physicalType == ParquetType.FIXED_LEN_BYTE_ARRAY) {
      result = 31 * result + typeLength;
    }
    return result;
  }

  @Override
  public org.apache.parquet.format.SchemaElement toParquet() {

    org.apache.parquet.format.SchemaElement element = new org.apache.parquet.format.SchemaElement();

    element.setName(name);
    element.setRepetition_type(convertEnum(org.apache.parquet.format.FieldRepetitionType.class, repetition));
    element.setType(convertEnum(org.apache.parquet.format.Type.class, physicalType));

    if (physicalType == ParquetType.FIXED_LEN_BYTE_ARRAY) {
      element.setType_length(typeLength);
    }

    if (decimalMetadata.isSet()) {
      element.setPrecision(decimalMetadata.precision());
      element.setScale(decimalMetadata.scale());
    }

    // TODO: Remove the guard on isInterval() to enable IntervalTypes after parquet.thrift recognizes them
    if (logicalType != null && logicalType.isSerialized() && !logicalType.isInterval()) {
      element.setLogicalType(logicalType.toThrift());
    }

    if (convertedType != ConvertedType.NONE && convertedType != ConvertedType.NA) {
      element.setConverted_type(convertEnum(org.apache.parquet.format.ConvertedType.class, convertedType));
    }

    if (convertedType == ConvertedType.NA) {
      // ConvertedType::NA is an unreleased, obsolete synonym for LogicalType::Null.
      // Never emit it (see PARQUET-1990 for discussion).
      if (logicalType == null || !logicalType.isNull()) {
        throw new ParquetException("ConvertedType::NA is obsolete, please use LogicalType::Null instead");
      }
    }

    if (fieldId >= 0) {
      element.setField_id(fieldId);
    }

    return element;
  }

  // TODO: Visitors
}
