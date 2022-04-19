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

public class ColumnProperties {

  private Encoding encoding_;
  private Compression codec_;
  private boolean dictionary_enabled_;
  private boolean statistics_enabled_;
  private long max_stats_size_;
  private int compression_level_;

  public ColumnProperties() {
    this.encoding_ = DefaultProperties.DEFAULT_ENCODING;
    this.codec_ = DefaultProperties.DEFAULT_COMPRESSION_TYPE;
    this.dictionary_enabled = DefaultProperties.DEFAULT_IS_DICTIONARY_ENABLED;
    this.statistics_enabled = DefaultProperties.DEFAULT_ARE_STATISTICS_ENABLED;
    this.max_stats_size = DefaultProperties.DEFAULT_MAX_STATISTICS_SIZE;
  }

  public ColumnProperties(
      Encoding encoding,
      Compression codec,
      boolean dictionary_enabled,
      boolean statistics_enabled,
      long max_stats_size) {

    this.encoding_ = encoding;
    this.codec_ = codec;
    this.dictionary_enabled_ = dictionary_enabled;
    this.statistics_enabled_ = statistics_enabled;
    this.max_stats_size_ = max_stats_size;
    this.compression_level_ = 0; // TODO  Codec::UseDefaultCompressionLevel();
  }

  void set_encoding(Encoding encoding) { 
    encoding_ = encoding; 
  }

  void set_compression(Compression codec) {
    codec_ = codec; 
  }

  void set_dictionary_enabled(boolean dictionary_enabled) {
    dictionary_enabled_ = dictionary_enabled;
  }

  void set_statistics_enabled(boolean statistics_enabled) {
    statistics_enabled_ = statistics_enabled;
  }

  void set_max_statistics_size(long max_stats_size) {
    max_stats_size_ = max_stats_size;
  }

  void set_compression_level(int compression_level) {
    compression_level_ = compression_level;
  }

  Encoding encoding() { return encoding_; }

  public Compression compression() { return codec_; }

  public boolean dictionary_enabled() {
    return dictionary_enabled_;
  }

  public boolean statistics_enabled() {
    return statistics_enabled_;
  }

  public long max_statistics_size() {
    return max_stats_size_;
  }

  public int compression_level() {
    return compression_level_;
  }
}
