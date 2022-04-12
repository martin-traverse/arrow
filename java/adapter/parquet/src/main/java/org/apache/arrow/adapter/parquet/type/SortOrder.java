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


/**
 * Sort order for page and column statistics.
 *
 * Types are associated with sort orders (e.g., UTF8 columns should use UNSIGNED)
 * and column stats are aggregated using a sort order. As of parquet-format version 2.3.1,
 * the order used to aggregate stats is always SIGNED and is not stored in the Parquet file.
 * These stats are discarded for types that need unsigned. See PARQUET-686.
 *
 * Reference:
 * parquet-mr/parquet-hadoop/src/main/java/org/apache/parquet/
 * format/converter/ParquetMetadataConverter.java
 */
public enum SortOrder {

  SIGNED,
  UNSIGNED,
  UNKNOWN;

  public static SortOrder getSortOrder(LogicalType logicalType, ParquetType primitive) {

    SortOrder o = SortOrder.UNKNOWN;

    if (logicalType != null && logicalType.isValid()) {

      o = (logicalType.isNone() ?
          defaultSortOrder(primitive) :
          logicalType.sortOrder());
    }

    return o;
  }

  public static SortOrder getSortOrder(ConvertedType converted, ParquetType primitive) {
    
    if (converted == ConvertedType.NONE) {
      return defaultSortOrder(primitive);
    }
    
    switch (converted) {
      case INT_8:
      case INT_16:
      case INT_32:
      case INT_64:
      case DATE:
      case TIME_MICROS:
      case TIME_MILLIS:
      case TIMESTAMP_MICROS:
      case TIMESTAMP_MILLIS:
        return SortOrder.SIGNED;
      case UINT_8:
      case UINT_16:
      case UINT_32:
      case UINT_64:
      case ENUM:
      case UTF8:
      case BSON:
      case JSON:
        return SortOrder.UNSIGNED;
      case DECIMAL:
      case LIST:
      case MAP:
      case MAP_KEY_VALUE:
      case INTERVAL:
      case NA:    // required instead of default
      case UNDEFINED:
      default:
        return SortOrder.UNKNOWN;
    }
  }

  // Return the Sort Order of the Parquet Physical Types
  public static SortOrder defaultSortOrder(ParquetType primitive) {

    switch (primitive) {
      case BOOLEAN:
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
        return SortOrder.SIGNED;
      case BYTE_ARRAY:
      case FIXED_LEN_BYTE_ARRAY:
        return SortOrder.UNSIGNED;
      case INT96:
      case UNDEFINED:
      default:
        return SortOrder.UNKNOWN;
    }
  }
}
