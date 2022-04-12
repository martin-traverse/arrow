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


/** Logical type class for decimal types. */
public class DecimalLogicalType extends LogicalType {

  private final int precision;
  private final int scale;

  /** Logical type class for decimal types. */
  public DecimalLogicalType(int precision, int scale) {

    super(LogicalType.Type.DECIMAL, SortOrder.SIGNED);

    if (precision < 1) {
      throw new ParquetException("Precision must be greater than or equal to 1 for Decimal logical type");
    }

    if (scale < 0 || scale > precision) {
      throw new ParquetException(
          "Scale must be a non-negative integer that does not exceed precision for Decimal logical type");
    }

    this.precision = precision;
    this.scale = scale;
  }

  public int precision() {
    return precision;
  }

  public int scale() {
    return scale;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) { return true; }
    if (!(o instanceof DecimalLogicalType)) { return false; }
    if (!super.equals(o)) { return false; }

    DecimalLogicalType that = (DecimalLogicalType) o;

    if (precision != that.precision) { return false; }
    return scale == that.scale;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + precision;
    result = 31 * result + scale;
    return result;
  }

  @Override
  public boolean isApplicable(ParquetType primitiveType, int primitiveLength /*= -1*/) {

    boolean ok = false;
    switch (primitiveType) {

      case INT32:
        ok = (1 <= precision) && (precision <= 9);
        break;

      case INT64:
        ok = (1 <= precision) && (precision <= 18);
        // FIXME(tpb): warn that INT32 could be used
        // if precision < 10, issue warning
        break;

      case FIXED_LEN_BYTE_ARRAY:
        ok = precision <= Math.floor(Math.log10(Math.pow(2.0, (8.0 * primitiveLength) - 1.0)));
        break;

      case BYTE_ARRAY:
        ok = true;
        break;

      default:
        break;
    }

    return ok;
  }

  @Override
  public boolean isCompatible(ConvertedType convertedType, DecimalMetadata convertedDecimalMetadata) {

    return convertedType == ConvertedType.DECIMAL &&
        (convertedDecimalMetadata.isSet() &&
        convertedDecimalMetadata.scale() == scale &&
        convertedDecimalMetadata.precision() == precision);
  }

  @Override
  public ConvertedType toConvertedType(DecimalMetadata outDecimalMetadata) {

    outDecimalMetadata.set(true, precision, scale);
    return ConvertedType.DECIMAL;
  }

  @Override
  public String toString() {

    return "Decimal(precision=" + precision + ", scale=" + scale + ")";
  }

  @Override
  public String toJson() {

    return "{\"Type\": \"Decimal\", \"precision\": " + precision + ", \"scale\": " + scale + "}";
  }

  @Override
  public org.apache.parquet.format.LogicalType toThrift() {

    org.apache.parquet.format.LogicalType type = new org.apache.parquet.format.LogicalType();
    org.apache.parquet.format.DecimalType decimalType = new org.apache.parquet.format.DecimalType();
    decimalType.setPrecision(precision);
    decimalType.setScale(scale);
    type.setDECIMAL(decimalType);
    return type;
  }
}
