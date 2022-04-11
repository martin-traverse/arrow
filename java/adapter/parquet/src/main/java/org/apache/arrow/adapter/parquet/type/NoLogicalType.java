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


/** Logical type class for objects with no type (explicit "none" type, which is different from undefined). */
public class NoLogicalType extends LogicalType {

  /** Logical type class for objects with no type (explicit "none" type, which is different from undefined). */
  public NoLogicalType() {

    super(LogicalType.Type.NONE, SortOrder.UNKNOWN,
        Compatability.SIMPLE_COMPATIBLE, Applicability.UNIVERSAL_APPLICABLE,
        ConvertedType.NONE);
  }

  @Override
  public String toString() {
    return "None";
  }
}