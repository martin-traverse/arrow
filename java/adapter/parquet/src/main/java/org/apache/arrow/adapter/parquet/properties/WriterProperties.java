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

import org.apache.arrow.adapter.parquet.ParquetException;
import org.apache.arrow.adapter.parquet.schema.ColumnPath;
import org.apache.arrow.adapter.parquet.type.Encoding;
import org.apache.arrow.adapter.parquet.type.ParquetVersion;

import java.util.Map;

public class WriterProperties {

  private final MemoryPool pool_;
  private final long dictionary_pagesize_limit_;
  private final long write_batch_size_;
  private final long max_row_group_length_;
  private final long pagesize_;
  private final ParquetDataPageVersion parquet_data_page_version_;
  private final ParquetVersion parquet_version_;
  private final String parquet_created_by_;
  private final FileEncryptionProperties file_encryption_properties_;
  private final ColumnProperties default_column_properties_;
  private final Map<String, ColumnProperties> column_properties_;
  
  private WriterProperties(
      MemoryPool pool, long dictionary_pagesize_limit, long write_batch_size,
      long max_row_group_length, long pagesize, ParquetVersion version,
      String created_by,
      FileEncryptionProperties file_encryption_properties,
      ColumnProperties default_column_properties,
      Map<String, ColumnProperties> column_properties,
      ParquetDataPageVersion data_page_version) {
    
    this.pool_ = pool;
    this.dictionary_pagesize_limit_ = dictionary_pagesize_limit;
    this.write_batch_size_ = write_batch_size;
    this.max_row_group_length_ = max_row_group_length;
    this.pagesize_ = pagesize;
    this.parquet_data_page_version_ = data_page_version;
    this.parquet_version_ = version;
    this.parquet_created_by_ = created_by;
    this.file_encryption_properties_ = file_encryption_properties;
    this.default_column_properties_ = default_column_properties;
    this.column_properties_ = column_properties;
  }

  public static class Builder {

    private MemoryPool pool_;
    private long dictionary_pagesize_limit_;
    private long write_batch_size_;
    private long max_row_group_length_;
    private long pagesize_;
    private ParquetVersion version_;
    private ParquetDataPageVersion data_page_version_;
    private String created_by_;

    private FileEncryptionProperties file_encryption_properties_;

    // Settings used for each column unless overridden in any of the maps below
    private ColumnProperties default_column_properties_;
    private Map<String, Encoding> encodings_;
    private Map<String, Compression> codecs_;
    private Map<String, Integer> codecs_compression_level_;
    private Map<String, Boolean> dictionary_enabled_;
    private Map<String, Boolean> statistics_enabled_;
    
    public Builder() {

      this.pool_ = null;  // ::arrow::default_memory_pool()),
      this.dictionary_pagesize_limit_ = DefaultProperties.DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT;
      this.write_batch_size_ = DefaultProperties.DEFAULT_WRITE_BATCH_SIZE;
      this.max_row_group_length_ = DefaultProperties.DEFAULT_MAX_ROW_GROUP_LENGTH;
      this.pagesize_ = DefaultProperties.kDefaultDataPageSize;
      this.version_ = ParquetVersion.PARQUET_1_0;
      this.data_page_version_ = ParquetDataPageVersion.V1;
      this.created_by_ = DefaultProperties.DEFAULT_CREATED_BY;
    }

    /// Specify the memory pool for the writer. Default default_memory_pool.
    public Builder memory_pool(MemoryPool pool) {
      pool_ = pool;
      return this;
    }

    /// Enable dictionary encoding in general for all columns. Default enabled.
    public Builder enable_dictionary() {
      default_column_properties_.set_dictionary_enabled(true);
      return this;
    }

    /// Disable dictionary encoding in general for all columns. Default enabled.
    public Builder disable_dictionary() {
      default_column_properties_.set_dictionary_enabled(false);
      return this;
    }

    /// Enable dictionary encoding for column specified by `path`. Default enabled.
    public Builder enable_dictionary(String path) {
      dictionary_enabled_.put(path, true);
      return this;
    }

    /// Enable dictionary encoding for column specified by `path`. Default enabled.
    public Builder enable_dictionary(ColumnPath path) {
      return enable_dictionary(path.toDotString());
    }

    /// Disable dictionary encoding for column specified by `path`. Default enabled.
    public Builder disable_dictionary(String path) {
      dictionary_enabled_.put(path, false);
      return this;
    }

    /// Disable dictionary encoding for column specified by `path`. Default enabled.
    public Builder disable_dictionary(ColumnPath path) {
      return disable_dictionary(path.toDotString());
    }

    /// Specify the dictionary page size limit per row group. Default 1MB.
    public Builder dictionary_pagesize_limit(long dictionary_psize_limit) {
      dictionary_pagesize_limit_ = dictionary_psize_limit;
      return this;
    }

    /// Specify the write batch size while writing batches of Arrow values into Parquet.
    /// Default 1024.
    public Builder write_batch_size(long write_batch_size) {
      write_batch_size_ = write_batch_size;
      return this;
    }

    /// Specify the max row group length.
    /// Default 64M.
    public Builder max_row_group_length(long max_row_group_length) {
      max_row_group_length_ = max_row_group_length;
      return this;
    }

    /// Specify the data page size.
    /// Default 1MB.
    public Builder data_pagesize(long pg_size) {
      pagesize_ = pg_size;
      return this;
    }

    /// Specify the data page version.
    /// Default V1.
    public Builder data_page_version(ParquetDataPageVersion data_page_version) {
      data_page_version_ = data_page_version;
      return this;
    }

    /// Specify the Parquet file version.
    /// Default PARQUET_1_0.
    public Builder version(ParquetVersion version) {
      version_ = version;
      return this;
    }

    public Builder created_by(String created_by) {
      created_by_ = created_by;
      return this;
    }

    /// \brief Define the encoding that is used when we don't utilise dictionary encoding.
    //
    /// This either apply if dictionary encoding is disabled or if we fallback
    /// as the dictionary grew too large.
    public Builder encoding(Encoding encoding_type) {
      if (encoding_type == Encoding.PLAIN_DICTIONARY ||
          encoding_type == Encoding.RLE_DICTIONARY) {
        throw new ParquetException("Can't use dictionary encoding as fallback encoding");
      }

      default_column_properties_.set_encoding(encoding_type);
      return this;
    }

    /// \brief Define the encoding that is used when we don't utilise dictionary encoding.
    //
    /// This either apply if dictionary encoding is disabled or if we fallback
    /// as the dictionary grew too large.
    public Builder encoding(String path, Encoding encoding_type) {
      if (encoding_type == Encoding.PLAIN_DICTIONARY ||
          encoding_type == Encoding.RLE_DICTIONARY) {
        throw new ParquetException("Can't use dictionary encoding as fallback encoding");
      }

      encodings_.put(path, encoding_type);
      return this;
    }

    /// \brief Define the encoding that is used when we don't utilise dictionary encoding.
    //
    /// This either apply if dictionary encoding is disabled or if we fallback
    /// as the dictionary grew too large.
    public Builder encoding(ColumnPath path, Encoding encoding_type) {
      return encoding(path.toDotString(), encoding_type);
    }

    /// Specify compression codec in general for all columns.
    /// Default UNCOMPRESSED.
    public Builder compression(Compression codec) {
      default_column_properties_.set_compression(codec);
      return this;
    }

    /// Specify max statistics size to store min max value.
    /// Default 4KB.
    public Builder max_statistics_size(long max_stats_sz) {
      default_column_properties_.set_max_statistics_size(max_stats_sz);
      return this;
    }

    /// Specify compression codec for the column specified by `path`.
    /// Default UNCOMPRESSED.
    public Builder compression(String path, Compression codec) {
      codecs_.put(path, codec);
      return this;
    }

    /// Specify compression codec for the column specified by `path`.
    /// Default UNCOMPRESSED.
    public Builder compression(ColumnPath path, Compression codec) {
      return compression(path.toDotString(), codec);
    }

    /// \brief Specify the default compression level for the compressor in
    /// every column.  In case a column does not have an explicitly specified
    /// compression level, the default one would be used.
    ///
    /// The provided compression level is compressor specific. The user would
    /// have to familiarize oneself with the available levels for the selected
    /// compressor.  If the compressor does not allow for selecting different
    /// compression levels, calling this function would not have any effect.
    /// Parquet and Arrow do not validate the passed compression level.  If no
    /// level is selected by the user or if the special
    /// std::numeric_limits<int>::min() value is passed, then Arrow selects the
    /// compression level.
    public Builder compression_level(int compression_level) {
      default_column_properties_.set_compression_level(compression_level);
      return this;
    }

    /// \brief Specify a compression level for the compressor for the column
    /// described by path.
    ///
    /// The provided compression level is compressor specific. The user would
    /// have to familiarize oneself with the available levels for the selected
    /// compressor.  If the compressor does not allow for selecting different
    /// compression levels, calling this function would not have any effect.
    /// Parquet and Arrow do not validate the passed compression level.  If no
    /// level is selected by the user or if the special
    /// std::numeric_limits<int>::min() value is passed, then Arrow selects the
    /// compression level.
    public Builder compression_level(String path, int compression_level) {
      codecs_compression_level_.put(path, compression_level);
      return this;
    }

    /// \brief Specify a compression level for the compressor for the column
    /// described by path.
    ///
    /// The provided compression level is compressor specific. The user would
    /// have to familiarize oneself with the available levels for the selected
    /// compressor.  If the compressor does not allow for selecting different
    /// compression levels, calling this function would not have any effect.
    /// Parquet and Arrow do not validate the passed compression level.  If no
    /// level is selected by the user or if the special
    /// std::numeric_limits<int>::min() value is passed, then Arrow selects the
    /// compression level.
    public Builder compression_level(ColumnPath path, int compression_level) {
      return compression_level(path.toDotString(), compression_level);
    }

    /// Define the file encryption properties.
    /// Default NULL.
    public Builder encryption(FileEncryptionProperties file_encryption_properties) {
      file_encryption_properties_ = file_encryption_properties;
      return this;
    }

    /// Enable statistics in general.
    /// Default enabled.
    public Builder enable_statistics() {
      default_column_properties_.set_statistics_enabled(true);
      return this;
    }

    /// Disable statistics in general.
    /// Default enabled.
    public Builder disable_statistics() {
      default_column_properties_.set_statistics_enabled(false);
      return this;
    }

    /// Enable statistics for the column specified by `path`.
    /// Default enabled.
    public Builder enable_statistics(String path) {
      statistics_enabled_.put(path, true);
      return this;
    }

    /// Enable statistics for the column specified by `path`.
    /// Default enabled.
    public Builder enable_statistics(ColumnPath path) {
      return enable_statistics(path.toDotString());
    }

    /// Disable statistics for the column specified by `path`.
    /// Default enabled.
    public Builder disable_statistics(String path) {
      statistics_enabled_.put(path, false);
      return this;
    }

    /// Disable statistics for the column specified by `path`.
    /// Default enabled.
    public Builder disable_statistics(ColumnPath path) {
      return disable_statistics(path.toDotString());
    }

    /// \brief Build the WriterProperties with the builder parameters.
    /// \return The WriterProperties defined by the builder.
    public WriterProperties build() {

      Map<String, ColumnProperties> column_properties;
      auto get = [&](const std::string& key) -> ColumnProperties& {
          auto it = column_properties.find(key);
      if (it == column_properties.end())
        return column_properties[key] = default_column_properties_;
      else
        return it->second;
      };

      for (const auto& item : encodings_) get(item.first).set_encoding(item.second);
      for (const auto& item : codecs_) get(item.first).set_compression(item.second);
      for (const auto& item : codecs_compression_level_)
      get(item.first).set_compression_level(item.second);
      for (const auto& item : dictionary_enabled_)
      get(item.first).set_dictionary_enabled(item.second);
      for (const auto& item : statistics_enabled_)
      get(item.first).set_statistics_enabled(item.second);

      return new WriterProperties(
          pool_, dictionary_pagesize_limit_, write_batch_size_, max_row_group_length_,
          pagesize_, version_, created_by_, std::move(file_encryption_properties_),
          default_column_properties_, column_properties, data_page_version_);
    }
  };

  public MemoryPool memory_pool() { return pool_; }

  public long dictionary_pagesize_limit() { return dictionary_pagesize_limit_; }

  public long write_batch_size() { return write_batch_size_; }

  public long max_row_group_length() { return max_row_group_length_; }

  public long data_pagesize() { return pagesize_; }

  public ParquetDataPageVersion data_page_version() {
    return parquet_data_page_version_;
  }

  public ParquetVersion version() { return parquet_version_; }

  public String created_by() { return parquet_created_by_; }

  public Encoding dictionary_index_encoding() {
    if (parquet_version_ == ParquetVersion.PARQUET_1_0) {
      return Encoding.PLAIN_DICTIONARY;
    } else {
      return Encoding.RLE_DICTIONARY;
    }
  }

  public Encoding dictionary_page_encoding() {
    if (parquet_version_ == ParquetVersion.PARQUET_1_0) {
      return Encoding.PLAIN_DICTIONARY;
    } else {
      return Encoding.PLAIN;
    }
  }

  private ColumnProperties column_properties(ColumnPath path) {

    ColumnProperties it = column_properties_.get(path.toDotString());
    if (it != null) { return it; }
    return default_column_properties_;
  }

  public Encoding encoding(ColumnPath path) {
    return column_properties(path).encoding();
  }

  public Compression compression(ColumnPath path) {
    return column_properties(path).compression();
  }

  public int compression_level(ColumnPath path) {
    return column_properties(path).compression_level();
  }

  public boolean dictionary_enabled(ColumnPath path) {
    return column_properties(path).dictionary_enabled();
  }

  public boolean statistics_enabled(ColumnPath path) {
    return column_properties(path).statistics_enabled();
  }

  public long max_statistics_size(ColumnPath path) {
    return column_properties(path).max_statistics_size();
  }

  public FileEncryptionProperties file_encryption_properties() {
    return file_encryption_properties_;
  }

  public ColumnEncryptionProperties column_encryption_properties(String path) {
    if (file_encryption_properties_) {
      return file_encryption_properties_.column_encryption_properties(path);
    } else {
      return NULLPTR;
    }
  }


}
