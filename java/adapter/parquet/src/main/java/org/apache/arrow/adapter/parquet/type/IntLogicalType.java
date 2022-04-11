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


public class IntLogicalType extends LogicalType {

    private final int width;
    private final boolean signed;

    public IntLogicalType(int width, boolean signed) {

        super(LogicalType.Type.INT, signed ? SortOrder.SIGNED : SortOrder.UNSIGNED,
                Compatability.CUSTOM_COMPATIBILITY, Applicability.CUSTOM_APPLICABILITY);

        this.width = width;
        this.signed = signed;
    }

    public int bitWidth() {
        return width;
    }

    public boolean isSigned() {
        return signed;
    }

    @Override
    public boolean isApplicable(ParquetType primitiveType, int primitiveLength) {

        return (primitiveType == ParquetType.INT32 && width <= 32) ||
                (primitiveType == ParquetType.INT64 && width == 64);
    }

    @Override
    public boolean isCompatible(ConvertedType convertedType, DecimalMetadata convertedDecimalMetadata) {

        if (convertedDecimalMetadata.isSet())
            return false;

        if (signed && width == 8)
            return convertedType == ConvertedType.INT_8;

        if (signed && width == 16)
            return convertedType == ConvertedType.INT_16;

        if (signed && width == 32)
            return convertedType == ConvertedType.INT_32;

        if (signed && width == 64)
            return convertedType == ConvertedType.INT_64;

        if (!signed && width == 8)
            return convertedType == ConvertedType.UINT_8;

        if (!signed && width == 16)
            return convertedType == ConvertedType.UINT_16;

        if (!signed && width == 32)
            return convertedType == ConvertedType.UINT_32;

        if (!signed && width == 64)
            return convertedType == ConvertedType.UINT_64;

        return false;
    }

    @Override
    public ConvertedType toConvertedType(DecimalMetadata outDecimalMetadata) {

        outDecimalMetadata.reset();

        if (signed) {
            switch (width) {
                case 8:
                    return ConvertedType.INT_8;
                case 16:
                    return ConvertedType.INT_16;
                case 32:
                    return ConvertedType.INT_32;
                case 64:
                    return ConvertedType.INT_64;
            }
        }
        else {  // unsigned
            switch (width) {
                case 8:
                    return ConvertedType.UINT_8;
                case 16:
                    return ConvertedType.UINT_16;
                case 32:
                    return ConvertedType.UINT_32;
                case 64:
                    return ConvertedType.UINT_64;
            }
        }

        return ConvertedType.NONE;
    }



    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();
        sb.append("Int(bitWidth=");
        sb.append(width);
        sb.append(", isSigned=");
        sb.append(signed);
        sb.append(")");

        return sb.toString();
    }

    @Override
    public String toJSON() {

        StringBuilder sb = new StringBuilder();
        sb.append("{\"Type\": \"Int\", \"bitWidth\": ");
        sb.append(width);
        sb.append(", \"isSigned\": ");
        sb.append(signed);
        sb.append("}");

        return sb.toString();
    }

    // TODO: toThrift

//    format::LogicalType LogicalType::Impl::Int::ToThrift() const {
//        format::LogicalType type;
//        format::IntType int_type;
//        DCHECK(width_ == 64 || width_ == 32 || width_ == 16 || width_ == 8);
//        int_type.__set_bitWidth(static_cast<int8_t>(width_));
//        int_type.__set_isSigned(signed_);
//        type.__set_INTEGER(int_type);
//        return type;
//    }

    @Override
    public boolean equals(LogicalType other) {

        boolean eq = false;

        if (other.isInt()) {
            IntLogicalType otherInt = (IntLogicalType) other;
            eq = width == otherInt.width && signed == otherInt.signed;
        }

        return eq;
    }
}
