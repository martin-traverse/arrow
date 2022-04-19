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

package org.apache.arrow.adapter.parquet.properties;

import org.apache.arrow.adapter.parquet.type.Encoding;

public class DefaultProperties {

  public static final long kDefaultDataPageSize = 1024 * 1024;

  public static final boolean DEFAULT_IS_DICTIONARY_ENABLED = true;
  public static final long DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT = kDefaultDataPageSize;
  public static final long DEFAULT_WRITE_BATCH_SIZE = 1024;
  public static final long DEFAULT_MAX_ROW_GROUP_LENGTH = 64 * 1024 * 1024;
  public static final boolean DEFAULT_ARE_STATISTICS_ENABLED = true;
  public static final long DEFAULT_MAX_STATISTICS_SIZE = 4096;
  public static final Encoding DEFAULT_ENCODING = Encoding::PLAIN;
  public static final char DEFAULT_CREATED_BY[] = CREATED_BY_VERSION;
  public static final Compression::type DEFAULT_COMPRESSION_TYPE = Compression::UNCOMPRESSED;
}
