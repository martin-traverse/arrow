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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.WritableByteChannel;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.arrow.adapter.parquet.ParquetException;
import org.apache.arrow.adapter.parquet.schema.ColumnOrder;
import org.apache.arrow.adapter.parquet.schema.SchemaDescriptor;
import org.apache.arrow.adapter.parquet.schema.SchemaNode;
import org.apache.arrow.adapter.parquet.thrift.ThriftHelpers;
import org.apache.arrow.adapter.parquet.type.ParquetVersion;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.RowGroup;


/** FileMetaData is a proxy around Thrift format FileMetaData. */
public class FileMetaData {

  private final org.apache.parquet.format.FileMetaData metadata;
  private final int metadataLength; // CPP is unsigned 32
  private final SchemaDescriptor schema;
  private final ApplicationVersion writerVersion;

  // TODO: Is it ok using list of map entries for KV metadata?
  private final List<Map.Entry<String, String>> keyValueMetadata;

  // std::shared_ptr<InternalFileDecryptor> fileDecryptor;

  /** Construct FileMetaData by deserializing a Thrift FileMetaData message from a buffer. */
  public FileMetaData(ArrowBuf metadataBuffer, Object fileDecryptor) {

    // auto footer_decryptor = file_decryptor_ != nullptr ? file_decryptor->GetFooterDecryptor() : nullptr;

    long metadataBufferStart = metadataBuffer.readerIndex();

    this.metadata = ThriftHelpers.deserializeThriftMsg(
        org.apache.parquet.format.FileMetaData::new,
        metadataBuffer, /* decryptor = */ null);

    this.metadataLength = (int) (metadataBuffer.readerIndex() - metadataBufferStart);

    this.writerVersion = metadata.isSetCreated_by() ?
        ApplicationVersion.fromVersionString(metadata.getCreated_by()) :
        ApplicationVersion.fromVersionString("unknown 0.0.0");

    this.schema = new SchemaDescriptor(SchemaNode.unflatten(metadata.schema));
    initColumnOrders();

    this.keyValueMetadata = initKeyValueMetadata();
  }

  private FileMetaData(
      org.apache.parquet.format.FileMetaData metadata, int metadataLength,
      SchemaDescriptor schema, ApplicationVersion writerVersion,
      List<Map.Entry<String, String>> keyValueMetadata) {

    this.metadata = metadata;
    this.metadataLength = metadataLength;
    this.schema = schema;
    this.writerVersion = writerVersion;
    this.keyValueMetadata = keyValueMetadata;
  }

  private void initColumnOrders() {

    // update ColumnOrder

    if (metadata.isSetColumn_orders()) {

      List<ColumnOrder> columnOrders = new ArrayList<>(schema.numColumns());
      for (org.apache.parquet.format.ColumnOrder columnOrder : metadata.getColumn_orders()) {
        columnOrders.add(columnOrder.isSetTYPE_ORDER() ? ColumnOrder.TYPE_DEFINED : ColumnOrder.UNDEFINED);
      }
      schema.updateColumnOrders(columnOrders);

    } else {
      schema.updateColumnOrders(Collections.nCopies(schema.numColumns(), ColumnOrder.UNDEFINED));
    }
  }

  private List<Map.Entry<String, String>> initKeyValueMetadata() {

    if (!metadata.isSetKey_value_metadata()) {
      return new ArrayList<>();
    }

    List<Map.Entry<String, String>> kvList = new ArrayList<>(metadata.getKey_value_metadataSize());

    for (KeyValue kv : metadata.getKey_value_metadata()) {
      kvList.add(new AbstractMap.SimpleEntry<>(kv.getKey(), kv.getValue()));
    }

    return kvList;
  }

  public SchemaDescriptor getSchema() {
    return schema;
  }

  /** Return the application's version of the writer. */
  public ApplicationVersion getWriterVersion() {
    return writerVersion;
  }

  @Override
  public boolean equals(Object o) {
    // Equivalent of CPP FileMetaData::Equals, which only considers the Thrift FileMetaData
    if (this == o) { return true; }
    if (!(o instanceof FileMetaData)) { return false; }
    FileMetaData that = (FileMetaData) o;
    return Objects.equals(metadata, that.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metadata);
  }

  /**
   * The number of top-level columns in the schema.
   *
   * Parquet thrift definition requires that nested schema elements are
   * flattened. This method returns the number of columns in the un-flattened
   * version.
   */
  int numColumns() {
    return schema.numColumns();
  }

  /**
   * The number of flattened schema elements.
   *
   * Parquet thrift definition requires that nested schema elements are
   * flattened. This method returns the total number of elements in the
   * flattened list.
   */
  int numSchemaElements() {
    return metadata.getSchemaSize();
  }

  /** The total number of rows. */
  long numRows() {
    return metadata.getNum_rows();
  }

  /** The number of row groups in the file. */
  int numRowGroups() {
    return metadata.getRow_groupsSize();
  }

  /**
   * Return the RowGroupMetaData of the corresponding row group ordinal.
   *
   * @param rowGroupIndex Index of the RowGroup to retrieve.
   * @throws ParquetException if the index is out of bound.
   */
  RowGroupMetaData rowGroup(int rowGroupIndex) {

    if (rowGroupIndex < 0 || rowGroupIndex >= numRowGroups()) {
      throw new ParquetException(
          "The file only has " + numRowGroups() +
          " row groups, requested metadata for row group: " + rowGroupIndex);
    }

    return new RowGroupMetaData(metadata.row_groups.get(rowGroupIndex), schema, writerVersion /*, file_decryptor*/);
  }

  /**
   * Return the "version" of the file.
   *
   * WARNING: The value returned by this method is unreliable as 1) the Parquet
   * file metadata stores the version as a single integer and 2) some producers
   * are known to always write a hardcoded value.  Therefore, you cannot use
   * this value to know which features are used in the file.
   */
  ParquetVersion version() {

    switch (metadata.version) {
      case 1:
        return ParquetVersion.PARQUET_1_0;
      case 2:
        return ParquetVersion.PARQUET_2_LATEST;
      default:
        // Improperly set version, assuming Parquet 1.0
        break;
    }

    return ParquetVersion.PARQUET_1_0;
  }

  /** Return the application's user-agent string of the writer. */
  String createdBy() {
    return metadata.getCreated_by();
  }

  /** Size of the original thrift encoded metadata footer. */
  int size() {
    return metadataLength;
  }

  /**
   * Indicate if all the FileMetaData's RowGroups can be decompressed.
   *
   * This will return false if any of the RowGroup's page is compressed with a
   * compression format which is not compiled in the current parquet library.
   */
  public boolean canDecompress() {

    for (int i = 0, n = numRowGroups(); i < n; ++i) {
      if (!rowGroup(i).canDecompress()) {
        return false;
      }
    }

    return true;
  }

  public boolean isEncryptionAlgorithmSet() {
    return metadata.isSetEncryption_algorithm();
  }

  public byte[] footerSigningKeyMetadata() {
    return metadata.getFooter_signing_key_metadata();
  }

  // TODO Encryption support
  // void set_file_decryptor(std::shared_ptr<InternalFileDecryptor> file_decryptor);
  // public EncryptionAlgorithm encryption_algorithm() {}
  /// \brief Verify signature of FileMetaData when file is encrypted but footer
  /// is not encrypted (plaintext footer).
  // public boolean verifySignature(byte[] signature) {}

  /** Serialize Thrift file metadata to an nio ByteChannel. */
  public void writeTo(WritableByteChannel dst /*, Encryptor encryptor */) {

    if (isEncryptionAlgorithmSet()) {
      throw new ParquetException("Encryption not available yet");
    }

    ThriftHelpers.serializeThriftMsg(metadata, dst, /* encryptor = */ null);
  }

  /** Serialize Thrift file metadata to an OutputStream. */
  public void writeTo(OutputStream dst /*, Encryptor encryptor */) {

    if (isEncryptionAlgorithmSet()) {
      throw new ParquetException("Encryption not available yet");
    }

    ThriftHelpers.serializeThriftMsg(metadata, dst, /* encryptor = */ null);
  }

  /** Return Thrift-serialized representation of the metadata as a byte array. */
  byte[] serializeToByteArray() {

    try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {

      writeTo(stream);
      return stream.toByteArray();

    } catch (IOException e) {
      throw new ParquetException(e.getMessage(), e);
    }
  }

  public List<Map.Entry<String, String>> keyValueMetadata() {
    return keyValueMetadata;
  }

  /**
   * Set a path to all ColumnChunk for all RowGroups.
   *
   * Commonly used by systems (Dask, Spark) who generates an metadata-only
   * parquet file. The path is usually relative to said index file.
   *
   * @param path The path to set.
   */
  void setFilePath(String path) {

    for (RowGroup rowGroup : metadata.getRow_groups()) {
      for (ColumnChunk columnChunk : rowGroup.getColumns()) {
        columnChunk.setFile_path(path);
      }
    }
  }

  /**
   * Merge row groups from another metadata file into this one.
   *
   * The schema of the input FileMetaData must be equal to the schema of this object.
   *
   * This is used by systems who creates an aggregate metadata-only file by
   * concatenating the row groups of multiple files. This newly created
   * metadata file acts as an index of all available row groups.
   *
   * @param other FileMetaData to merge the row groups from.
   * @throws ParquetException if schemas are not equal.
   */
  void appendRowGroups(FileMetaData other) {

    if (!getSchema().equals(other.getSchema())) {
      throw new ParquetException("AppendRowGroups requires equal schemas.");
    }

    // ARROW-13654: `other` may point to self, be careful not to enter an infinite loop
    int n = other.numRowGroups();

    for (int i = 0; i < n; i++) {
      RowGroup otherRg = other.metadata.getRow_groups().get(i);
      metadata.setNum_rows(metadata.getNum_rows() + otherRg.getNum_rows());
      metadata.getRow_groups().add(otherRg);
    }
  }

  /** Return a FileMetaData containing a subset of the row groups in this FileMetaData. */
  FileMetaData subset(List<Integer> rowGroups) {

    for (int i : rowGroups) {
      if (i < 0 || i >= numRowGroups()) {
        throw new ParquetException(
            "The file only has " + numRowGroups() +
            " row groups, but requested a subset including row group: " + i);
      }
    }

    org.apache.parquet.format.FileMetaData newMetadata = new org.apache.parquet.format.FileMetaData();
    newMetadata.setVersion(metadata.getVersion());
    newMetadata.setSchema(metadata.getSchema());

    newMetadata.setRow_groups(new ArrayList<>(rowGroups.size()));
    
    for (int rgIndex : rowGroups) {
      RowGroup rg = newMetadata.getRow_groups().get(rgIndex);
      newMetadata.setNum_rows(newMetadata.getNum_rows() + rg.getNum_rows());
      newMetadata.getRow_groups().add(rg);
    }

    newMetadata.setKey_value_metadata(metadata.getKey_value_metadata());
    newMetadata.setCreated_by(metadata.getCreated_by());
    newMetadata.setColumn_orders(metadata.getColumn_orders());
    newMetadata.setEncryption_algorithm(metadata.getEncryption_algorithm());
    newMetadata.setFooter_signing_key_metadata(metadata.getFooter_signing_key_metadata());

    return new FileMetaData(
        newMetadata, /* metadataLength = */ 0,
        schema, writerVersion, keyValueMetadata
        /*, decryptor */);
  }
}
