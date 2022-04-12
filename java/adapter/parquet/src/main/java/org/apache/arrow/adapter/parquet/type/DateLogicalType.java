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


/** Logical type class for date type. */
public class DateLogicalType extends LogicalType {

  /** Logical type class for date type. */
  public DateLogicalType() {

    super(Type.DATE, SortOrder.SIGNED,
        Compatability.SIMPLE_COMPATIBLE, Applicability.SIMPLE_APPLICABLE,
        ConvertedType.DATE, ParquetType.INT32);
  }

  @Override
  public String toString() {
    return "Date";
  }

  @Override
  public org.apache.parquet.format.LogicalType toThrift() {

    org.apache.parquet.format.LogicalType type = new org.apache.parquet.format.LogicalType();
    org.apache.parquet.format.DateType subType = new org.apache.parquet.format.DateType();
    type.setDATE(subType);
    return type;
  }
}
