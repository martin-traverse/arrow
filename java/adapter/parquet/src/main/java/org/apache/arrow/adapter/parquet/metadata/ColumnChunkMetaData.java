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

package org.apache.arrow.adapter.parquet.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.arrow.adapter.parquet.PageType;
import org.apache.arrow.adapter.parquet.ParquetException;
import org.apache.arrow.adapter.parquet.schema.ColumnDescriptor;
import org.apache.arrow.adapter.parquet.schema.ColumnPath;
import org.apache.arrow.adapter.parquet.statistics.EncodedStatistics;
import org.apache.arrow.adapter.parquet.statistics.Statistics;
import org.apache.arrow.adapter.parquet.type.Encoding;
import org.apache.arrow.adapter.parquet.type.ParquetType;
import org.apache.arrow.adapter.parquet.type.SortOrder;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnCryptoMetaData;
import org.apache.parquet.format.ColumnMetaData;


/** ColumnChunkMetaData is a proxy around Thrift format ColumnChunkMetaData. */
public class ColumnChunkMetaData {

  private final List<Encoding> encodings;
  private final List<PageEncodingStats> encodingStats;
  private final ColumnChunk column;
  private final ColumnMetaData columnMetaData;
  private final ColumnDescriptor descriptor;
  private final ApplicationVersion writerVersion;

  private Statistics possibleStats;

  // Only used during initialization when metadata is encrypted
  // private final ColumnMetaData decryptedMetadata;

  /** ColumnChunkMetaData is a proxy around Thrift format ColumnChunkMetaData. */
  public ColumnChunkMetaData(
      ColumnChunk column,
      ColumnDescriptor descr,
      short rowGroupOrdinal, short columnOrdinal,
      ApplicationVersion writerVersion
      /*, InternalFileDecryptor file_decryptor */ ) {

    this.column = column;
    this.descriptor = descr;
    this.writerVersion = writerVersion;

    this.columnMetaData = column.getMeta_data();

    // TODO: Encryption support

    if (column.isSetCrypto_metadata()) { // column metadata is encrypted

      ColumnCryptoMetaData ccmd = column.getCrypto_metadata();

      if (ccmd.isSetENCRYPTION_WITH_COLUMN_KEY()) {

        // TODO: CPP impl is in metadata.cc, line 186

        throw new ParquetException(
            "Cannot decrypt ColumnMetadata." +
            " FileDecryption is not setup correctly");
      }
    }

    List<Encoding> encodings = new ArrayList<>();
    List<PageEncodingStats> encodingStats = new ArrayList<>();

    for (org.apache.parquet.format.Encoding encoding : columnMetaData.getEncodings()) {
      encodings.add(convertEnum(encoding, Encoding.class));
    }

    for (org.apache.parquet.format.PageEncodingStats encodingStat : columnMetaData.getEncoding_stats()) {
      encodingStats.add(new PageEncodingStats(
          convertEnum(encodingStat.getPage_type(), PageType.class),
          convertEnum(encodingStat.getEncoding(), Encoding.class),
          encodingStat.getCount()));
    }

    this.encodings = Collections.unmodifiableList(encodings);
    this.encodingStats = Collections.unmodifiableList(encodingStats);

    possibleStats = null;
  }

  public List<Encoding> getEncodings() {
    return encodings;
  }

  public List<PageEncodingStats> getEncodingStats() {
    return encodingStats;
  }

  @Override
  public boolean equals(Object o) {

    // Based on CPP ColumnChunkMetaDataImpl which only compares columnMetadata

    if (this == o) { return true; }
    if (!(o instanceof ColumnChunkMetaData)) { return false; }
    ColumnChunkMetaData that = (ColumnChunkMetaData) o;
    return Objects.equals(columnMetaData, that.columnMetaData);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnMetaData);
  }


  private <T extends Enum<T>, S extends Enum<S>> T convertEnum(S source, Class<T> target) {
    return Enum.valueOf(target, source.name());
  }

  // column chunk
  public long fileOffset() {
    return column.getFile_offset();
  }

  // parameter is only used when a dataset is spread across multiple files
  public String filePath() {
    return column.getFile_path();
  }

  // column metadata
  public boolean isMetadataSet() {
    return column.isSetMeta_data();
  }

  public ParquetType type() {
    return convertEnum(columnMetaData.getType(), ParquetType.class);
  }

  public long numValues() {
    return columnMetaData.getNum_values();
  }

  public ColumnPath pathInSchema() {
    return new ColumnPath(columnMetaData.getPath_in_schema());
  }

  public boolean hasDictionaryPage() {
    return columnMetaData.isSetDictionary_page_offset();
  }

  public long dictionary_page_offset() {
    return columnMetaData.getDictionary_page_offset();
  }

  public long data_page_offset() {
    return columnMetaData.getData_page_offset();
  }

  public boolean has_index_page() {
    return columnMetaData.isSetIndex_page_offset();
  }

  public long index_page_offset() {
    return columnMetaData.getIndex_page_offset();
  }

  public long total_compressed_size() {
    return columnMetaData.getTotal_compressed_size();
  }

  public long total_uncompressed_size() {
    return columnMetaData.getTotal_uncompressed_size();
  }

  /** Check if statistics are set and are valid. */
  public boolean isStatsSet() {

    // Check if statistics are set and are valid
    // 1) Must be set in the metadata
    // 2) Statistics must not be corrupted

    if (writerVersion == null) {
      throw new ParquetException("Check failed: \"writerVersion != null\"");
    }

    // If the column statistics don't exist or column sort order is unknown
    // we cannot use the column stats
    if (!columnMetaData.isSetStatistics() || descriptor.sortOrder() == SortOrder.UNKNOWN) {
      return false;
    }

    if (possibleStats == null) {
      possibleStats = Statistics.makeColumnStats(columnMetaData, descriptor);
    }

    EncodedStatistics encodedStatistics = possibleStats.encode();

    return writerVersion.hasCorrectStatistics(type(), encodedStatistics, descriptor.sortOrder());
  }

  public Statistics statistics() {
    return isStatsSet() ? possibleStats : null;
  }

  // TODO: Compression
  // Compression::type compression() const;
  // Indicate if the ColumnChunk compression is supported by the current
  // compiled parquet library.
  // bool can_decompress() const;

  // TODO: Crypto support
  // ColumnCryptoMetaData cryptoMetadata() {}
}
