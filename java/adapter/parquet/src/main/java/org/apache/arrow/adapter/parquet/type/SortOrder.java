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
  UNKNOWN
}
