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

package org.apache.arrow.adapter.parquet.statistics;

import org.apache.arrow.adapter.parquet.ParquetException;
import org.apache.arrow.adapter.parquet.schema.ColumnDescriptor;
import org.apache.parquet.format.ColumnMetaData;


/** Base type for computing column statistics while writing a file. */
public abstract class Statistics {

  // TODO: Statistics

  /** Make a typed statistics object depending on the type of the column. */
  public static Statistics makeColumnStats(ColumnMetaData metadata, ColumnDescriptor descr) {
    
    // TODO: CPP impl is in metadata.cc, line 101

    throw new ParquetException("Can't decode page statistics for selected column type");
  }

  public EncodedStatistics encode() {
    return null;
  }
}
