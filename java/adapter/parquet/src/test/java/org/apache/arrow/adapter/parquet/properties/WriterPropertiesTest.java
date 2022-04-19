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

import org.apache.arrow.adapter.parquet.schema.ColumnPath;
import org.apache.arrow.adapter.parquet.type.Encoding;
import org.apache.arrow.adapter.parquet.type.ParquetVersion;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class WriterPropertiesTest {

  @Test
  void basics() {
  
    WriterProperties props = new WriterProperties.Builder().build();

    assertEquals(DefaultProperties.kDefaultDataPageSize, props.data_pagesize());
    assertEquals(DefaultProperties.DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT, props.dictionary_pagesize_limit());
    assertEquals(ParquetVersion.PARQUET_1_0, props.version());
    assertEquals(ParquetDataPageVersion.V1, props.data_page_version());
  }
  
  @Test
  void advancedHandling() {

    WriterProperties.Builder builder = new WriterProperties.Builder();

    builder.compression("gzip", Compression::GZIP);
    builder.compression("zstd", Compression::ZSTD);
    builder.compression(Compression::SNAPPY);
    builder.encoding(Encoding.DELTA_BINARY_PACKED);
    builder.encoding("delta-length", Encoding.DELTA_LENGTH_BYTE_ARRAY);
    builder.data_page_version(ParquetDataPageVersion.V2);
    WriterProperties props = builder.build();

    assertEquals(Compression::GZIP, props.compression(ColumnPath.fromDotString("gzip")));
    assertEquals(Compression::ZSTD, props.compression(ColumnPath.fromDotString("zstd")));
    assertEquals(Compression::SNAPPY,
    props.compression(ColumnPath.fromDotString("delta-length")));
    assertEquals(Encoding.DELTA_BINARY_PACKED, props.encoding(ColumnPath.fromDotString("gzip")));
    assertEquals(Encoding.DELTA_LENGTH_BYTE_ARRAY, props.encoding(ColumnPath.fromDotString("delta-length")));
    assertEquals(ParquetDataPageVersion.V2, props.data_page_version());
  }
}
