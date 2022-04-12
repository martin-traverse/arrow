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


/** Logical type class for time type. */
public class TimeLogicalType extends LogicalType {

  private final boolean isAdjustedToUtc;
  private final TimeUnit unit;

  /** Logical type class for time type. */
  public TimeLogicalType(boolean isAdjustedToUtc, TimeUnit unit) {

    super(Type.TIME, SortOrder.SIGNED);

    if (unit != TimeUnit.MILLIS &&
        unit != TimeUnit.MICROS &&
        unit != TimeUnit.NANOS) {

      throw new ParquetException("TimeUnit must be one of MILLIS, MICROS, or NANOS for Time logical type");
    }

    this.isAdjustedToUtc = isAdjustedToUtc;
    this.unit = unit;
  }

  public boolean isAdjustedToUtc() {
    return isAdjustedToUtc;
  }

  LogicalType.TimeUnit timeUnit() {
    return unit;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) { return true; }
    if (!(o instanceof TimeLogicalType)) { return false; }
    if (!super.equals(o)) { return false; }

    TimeLogicalType that = (TimeLogicalType) o;

    if (isAdjustedToUtc != that.isAdjustedToUtc) { return false; }
    return unit == that.unit;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (isAdjustedToUtc ? 1 : 0);
    result = 31 * result + (unit != null ? unit.hashCode() : 0);
    return result;
  }

  @Override
  public boolean isApplicable(ParquetType primitiveType, int primitiveLength) {

    return (primitiveType == ParquetType.INT32 && unit == TimeUnit.MILLIS) ||
        (primitiveType == ParquetType.INT64 && (unit == TimeUnit.MICROS || unit == TimeUnit.NANOS));
  }

  @Override
  public boolean isCompatible(ConvertedType convertedType, DecimalMetadata convertedDecimalMetadata) {

    if (convertedDecimalMetadata.isSet()) {
      return false;
    } else if (isAdjustedToUtc && unit == TimeUnit.MILLIS) {
      return convertedType == ConvertedType.TIME_MILLIS;
    } else if (isAdjustedToUtc && unit == TimeUnit.MICROS) {
      return convertedType == ConvertedType.TIME_MICROS;
    } else {
      return (convertedType == ConvertedType.NONE) || (convertedType == ConvertedType.NA);
    }
  }

  @Override
  public ConvertedType toConvertedType() {

    if (isAdjustedToUtc) {
      if (unit == TimeUnit.MILLIS) {
        return ConvertedType.TIME_MILLIS;
      } else if (unit == TimeUnit.MICROS) {
        return ConvertedType.TIME_MICROS;
      }
    }

    return ConvertedType.NONE;
  }

  @Override
  public String toString() {

    return "Time(isAdjustedToUTC=" + isAdjustedToUtc +
        ", timeUnit=" + unit.displayName() +
        ")";
  }

  @Override
  public String toJson() {

    return "{\"Type\": \"Time\", \"isAdjustedToUTC\": " + isAdjustedToUtc +
        ", \"timeUnit\": \"" + unit.displayName() +
        "\"}";
  }

  @Override
  public org.apache.parquet.format.LogicalType toThrift() {

    org.apache.parquet.format.LogicalType type = new org.apache.parquet.format.LogicalType();
    org.apache.parquet.format.TimeType timeType = new org.apache.parquet.format.TimeType();
    org.apache.parquet.format.TimeUnit timeUnit = new org.apache.parquet.format.TimeUnit();

    if (this.unit == LogicalType.TimeUnit.MILLIS) {
      timeUnit.setMILLIS(new org.apache.parquet.format.MilliSeconds());
    } else if (this.unit == LogicalType.TimeUnit.MICROS) {
      timeUnit.setMICROS(new org.apache.parquet.format.MicroSeconds());
    } else if (this.unit == LogicalType.TimeUnit.NANOS) {
      timeUnit.setNANOS(new org.apache.parquet.format.NanoSeconds());
    }

    timeType.setIsAdjustedToUTC(isAdjustedToUtc);
    timeType.setUnit(timeUnit);
    type.setTIME(timeType);

    return type;
  }
}
