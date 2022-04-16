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

package org.apache.arrow.adapter.parquet;

/** Parquet data encodings, mirrors parquet::Encoding. */
public enum Encoding {

  PLAIN, // = 0,

  /** This encoding is deprecated, it was never used. */
  @Deprecated
  __GROUP_VAR_INT, // = 1;

  PLAIN_DICTIONARY, // = 2,
  RLE, // = 3,
  BIT_PACKED, // = 4,
  DELTA_BINARY_PACKED, // = 5,
  DELTA_LENGTH_BYTE_ARRAY, // = 6,
  DELTA_BYTE_ARRAY, // = 7,
  RLE_DICTIONARY, // = 8,
  BYTE_STREAM_SPLIT, // = 9,

  // Should always be last element (except UNKNOWN)
  UNDEFINED, // = 10,
  UNKNOWN, // = 999
}
