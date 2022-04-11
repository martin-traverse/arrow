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

  private final boolean adjusted;
  private final TimeUnit timeUnit;

  /** Logical type class for time type. */
  public TimeLogicalType(boolean adjusted, TimeUnit timeUnit) {

    super(Type.TIME, SortOrder.SIGNED);

    if (timeUnit != TimeUnit.MILLIS &&
        timeUnit != TimeUnit.MICROS &&
        timeUnit != TimeUnit.NANOS) {

      throw new ParquetException("TimeUnit must be one of MILLIS, MICROS, or NANOS for Time logical type");
    }

    this.adjusted = adjusted;
    this.timeUnit = timeUnit;
  }

  public boolean isAdjustedToUtc() {
    return adjusted;
  }

  LogicalType.TimeUnit timeUnit() {
    return timeUnit;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) { return true; }
    if (!(o instanceof TimeLogicalType)) { return false; }
    if (!super.equals(o)) { return false; }

    TimeLogicalType that = (TimeLogicalType) o;

    if (adjusted != that.adjusted) { return false; }
    return timeUnit == that.timeUnit;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (adjusted ? 1 : 0);
    result = 31 * result + (timeUnit != null ? timeUnit.hashCode() : 0);
    return result;
  }

  @Override
  public boolean isApplicable(ParquetType primitiveType, int primitiveLength) {

    return (primitiveType == ParquetType.INT32 && timeUnit == TimeUnit.MILLIS) ||
        (primitiveType == ParquetType.INT64 && (timeUnit == TimeUnit.MICROS || timeUnit == TimeUnit.NANOS));
  }

  @Override
  public boolean isCompatible(ConvertedType convertedType, DecimalMetadata convertedDecimalMetadata) {

    if (convertedDecimalMetadata.isSet()) {
      return false;
    } else if (adjusted && timeUnit == TimeUnit.MILLIS) {
      return convertedType == ConvertedType.TIME_MILLIS;
    } else if (adjusted && timeUnit == TimeUnit.MICROS) {
      return convertedType == ConvertedType.TIME_MICROS;
    } else {
      return (convertedType == ConvertedType.NONE) || (convertedType == ConvertedType.NA);
    }
  }

  @Override
  public ConvertedType toConvertedType(DecimalMetadata outDecimalMetadata) {

    outDecimalMetadata.reset();

    if (adjusted) {
      if (timeUnit == TimeUnit.MILLIS) {
        return ConvertedType.TIME_MILLIS;
      } else if (timeUnit == TimeUnit.MICROS) {
        return ConvertedType.TIME_MICROS;
      }
    }

    return ConvertedType.NONE;
  }

  @Override
  public String toString() {

    return "Time(isAdjustedToUTC=" + adjusted +
        ", timeUnit=" + timeUnit.displayName() +
        ")";
  }

  @Override
  public String toJson() {

    return "{\"Type\": \"Time\", \"isAdjustedToUTC\": " + adjusted +
        ", \"timeUnit\": \"" + timeUnit.displayName() +
        "\"}";
  }

  // TODO: Thrift
}
