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


/** Mirrors parquet::Type. */
public enum ParquetType {

  BOOLEAN, // = 0,
  INT32, // = 1,
  INT64, // = 2,
  INT96, // = 3,
  FLOAT, // = 4,
  DOUBLE, // = 5,
  BYTE_ARRAY, // = 6,
  FIXED_LEN_BYTE_ARRAY, // = 7,

  // Should always be last element.
  UNDEFINED
}
