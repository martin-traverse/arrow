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

import java.util.Objects;

import org.apache.arrow.adapter.parquet.ParquetException;
import org.apache.arrow.adapter.parquet.schema.SchemaDescriptor;
import org.apache.parquet.format.RowGroup;


/** FileMetaData is a proxy around Thrift format FileMetaData. */
public class RowGroupMetadata {

  private final RowGroup rowGroup;
  private final SchemaDescriptor schema;
  private final ApplicationVersion writerVersion;

  // TODO: Encryption support
  // private final InternalFileDecryptor file_decryptor_;

  /** Create a RowGroupMetaData from a serialized thrift message. */
  public RowGroupMetadata(
      RowGroup rowGroup, SchemaDescriptor schema,
      ApplicationVersion writerVersion
      /*, InternalFileDecryptor file_decryptor */) {

    this.rowGroup = rowGroup;
    this.schema = schema;
    this.writerVersion = writerVersion; // CPP default: null
  }

  // Schema descriptor is immutable so safe to return.
  SchemaDescriptor getSchema() {
    return schema;
  }

  @Override
  public boolean equals(Object o) {

    // Based on CPP impl in RowGroupMetaDataImpl which only compares the Thrift row group

    if (this == o) { return true; }
    if (o == null || getClass() != o.getClass()) { return false; }
    RowGroupMetadata that = (RowGroupMetadata) o;
    return Objects.equals(rowGroup, that.rowGroup);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rowGroup);
  }

  /**
   * The number of columns in this row group.
   *
   * The order must match the parent's column ordering.
   */
  int num_columns() {
    return rowGroup.getColumnsSize();
  }

  /** Number of rows in this row group. */
  long num_rows() {
    return rowGroup.getNum_rows();
  }

  /** Total byte size of all the uncompressed column data in this row group. */
  long total_byte_size() {
    return rowGroup.getTotal_byte_size();
  }

  /**
   * Total byte size of all the compressed (and potentially encrypted column data in this row group.
   *
   * his information is optional and may be 0 if omitted.
   */
  long total_compressed_size() {
    return rowGroup.getTotal_compressed_size();
  }

  /**
   * Byte offset from beginning of file to first page (data or dictionary) in this row group.
   *
   * The file_offset field that this method exposes is optional.
   * This method will return 0 if that field is not set to a meaningful value.
   */
  long file_offset() {
    return rowGroup.getFile_offset();
  }

  /**
   * Return the ColumnChunkMetaData of the corresponding column ordinal.
   *
   * @param columnIndex Index of the ColumnChunkMetaData to retrieve.
   * @return The ColumnChunkMetaData of the corresponding column ordinal.
   * @throws ParquetException if the columnIndex is out of bound.
   */
  ColumnChunkMetadata columnChunk(int columnIndex) {

    if (0 < columnIndex && columnIndex < num_columns()) {

      return new ColumnChunkMetadata(
          rowGroup.getColumns().get(columnIndex), schema.column(columnIndex),
          rowGroup.getOrdinal(), (short) columnIndex,
          writerVersion /*, file_decryptor_ */);
    }
    throw new ParquetException(
        "The file only has " + num_columns() +
        " columns, requested metadata for column: " + columnIndex);
  }


  // TODO: Compression support
  /* Indicate if all of the RowGroup's ColumnChunks can be decompressed. */
  // boolean can_decompress() {
  //
  //    int n_columns = num_columns();
  //
  //    for (int i = 0; i < n_columns; i++) {
  //      if ( ColumnChunk(i).canDecompress()) {
  //        return false;
  //      }
  //    }
  //
  //    return true;
  //  }

}
