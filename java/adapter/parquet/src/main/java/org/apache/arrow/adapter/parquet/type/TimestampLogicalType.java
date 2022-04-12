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


/** Logical type class for timestamp types. */
public class TimestampLogicalType extends LogicalType {

  private final boolean isAdjustedToUtc;
  private final TimeUnit unit;

  private final boolean isFromConvertedType;
  private final boolean forceSetConvertedType;

  /** Logical type class for int types. */
  public TimestampLogicalType(
      boolean isAdjustedToUtc, TimeUnit unit,
      boolean isFromConvertedType, boolean forceSetConvertedType) {

    super(Type.TIMESTAMP, SortOrder.SIGNED,
        Compatability.CUSTOM_COMPATIBILITY, Applicability.SIMPLE_APPLICABLE,
        ParquetType.INT64);

    if (unit != TimeUnit.MILLIS &&
        unit != TimeUnit.MICROS &&
        unit != TimeUnit.NANOS) {

      throw new ParquetException("TimeUnit must be one of MILLIS, MICROS, or NANOS for Timestamp logical type");
    }

    this.isAdjustedToUtc = isAdjustedToUtc;
    this.unit = unit;
    this.isFromConvertedType = isFromConvertedType;
    this.forceSetConvertedType = forceSetConvertedType;
  }

  public boolean isAdjustedToUtc() {
    return isAdjustedToUtc;
  }

  public TimeUnit timeUnit() {
    return unit;
  }

  public boolean isFromConvertedType() {
    return isFromConvertedType;
  }

  public boolean forceSetConvertedType() {
    return forceSetConvertedType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) { return true; }
    if (!(o instanceof TimestampLogicalType)) { return false; }
    if (!super.equals(o)) { return false; }

    TimestampLogicalType that = (TimestampLogicalType) o;

    if (isAdjustedToUtc != that.isAdjustedToUtc) { return false; }
    if (isFromConvertedType != that.isFromConvertedType) { return false; }
    if (forceSetConvertedType != that.forceSetConvertedType) { return false; }
    return unit == that.unit;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (isAdjustedToUtc ? 1 : 0);
    result = 31 * result + (unit != null ? unit.hashCode() : 0);
    result = 31 * result + (isFromConvertedType ? 1 : 0);
    result = 31 * result + (forceSetConvertedType ? 1 : 0);
    return result;
  }

  @Override
  public boolean isSerialized() {
    return !isFromConvertedType;
  }

  @Override
  public boolean isCompatible(ConvertedType convertedType, DecimalMetadata convertedDecimalMetadata) {

    if (convertedDecimalMetadata.isSet()) {
      return false;
    } else if (unit == TimeUnit.MILLIS) {
      if (isAdjustedToUtc || forceSetConvertedType) {
        return convertedType == ConvertedType.TIMESTAMP_MILLIS;
      } else {
        return (convertedType == ConvertedType.NONE) || (convertedType == ConvertedType.NA);
      }
    } else if (unit == TimeUnit.MICROS) {
      if (isAdjustedToUtc || forceSetConvertedType) {
        return convertedType == ConvertedType.TIMESTAMP_MICROS;
      } else {
        return (convertedType == ConvertedType.NONE) || (convertedType == ConvertedType.NA);
      }
    } else {
      return (convertedType == ConvertedType.NONE) || (convertedType == ConvertedType.NA);
    }
  }

  @Override
  public ConvertedType toConvertedType(DecimalMetadata outDecimalMetadata) {

    outDecimalMetadata.reset();

    if (isAdjustedToUtc || forceSetConvertedType) {
      if (unit == TimeUnit.MILLIS) {
        return ConvertedType.TIMESTAMP_MILLIS;
      } else if (unit == TimeUnit.MICROS) {
        return ConvertedType.TIMESTAMP_MICROS;
      }
    }

    return ConvertedType.NONE;

  }

  @Override
  public String toString() {

    return "Timestamp(isAdjustedToUTC=" + isAdjustedToUtc +
        ", timeUnit=" + unit.displayName() +
        ", is_from_converted_type=" + isFromConvertedType +
        ", force_set_converted_type=" + forceSetConvertedType + ")";
  }

  @Override
  public String toJson() {

    return "{\"Type\": \"Timestamp\", \"isAdjustedToUTC\": " + isAdjustedToUtc +
        ", \"timeUnit\": \"" + unit.displayName() + "\"" +
        ", \"is_from_converted_type\": " + isFromConvertedType +
        ", \"force_set_converted_type\": " + forceSetConvertedType + "}";
  }

  @Override
  public org.apache.parquet.format.LogicalType toThrift() {

    org.apache.parquet.format.LogicalType type = new org.apache.parquet.format.LogicalType();
    org.apache.parquet.format.TimestampType timestampType = new org.apache.parquet.format.TimestampType();
    org.apache.parquet.format.TimeUnit timeUnit = new org.apache.parquet.format.TimeUnit();

    if (this.unit == TimeUnit.MILLIS) {
      timeUnit.setMILLIS(new org.apache.parquet.format.MilliSeconds());
    } else if (this.unit == TimeUnit.MICROS) {
      timeUnit.setMICROS(new org.apache.parquet.format.MicroSeconds());
    } else if (this.unit == TimeUnit.NANOS) {
      timeUnit.setNANOS(new org.apache.parquet.format.NanoSeconds());
    }

    timestampType.setIsAdjustedToUTC(isAdjustedToUtc);
    timestampType.setUnit(timeUnit);
    type.setTIMESTAMP(timestampType);

    return type;
  }
}
