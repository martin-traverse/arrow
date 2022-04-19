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

import java.util.Arrays;
import java.util.List;

import org.apache.arrow.adapter.parquet.properties.DefaultProperties;
import org.apache.arrow.adapter.parquet.properties.WriterProperties;
import org.apache.arrow.adapter.parquet.schema.SchemaDescriptor;
import org.apache.arrow.adapter.parquet.schema.SchemaGroupNode;
import org.apache.arrow.adapter.parquet.schema.SchemaNode;
import org.apache.arrow.adapter.parquet.schema.SchemaPrimitiveNode;
import org.apache.arrow.adapter.parquet.statistics.EncodedStatistics;
import org.apache.arrow.adapter.parquet.type.ParquetType;
import org.apache.arrow.adapter.parquet.type.ParquetVersion;
import org.apache.arrow.adapter.parquet.type.RepetitionType;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.MemoryUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


class MetadataTest {

  // Helper function for generating table metadata
  FileMetaData generateTableMetaData(
    SchemaDescriptor schema,
    WriterProperties props,
    long nrows,
    EncodedStatistics stats_int,
    EncodedStatistics stats_float) {

    auto f_builder = FileMetaDataBuilder::Make(&schema, props);
    auto rg1_builder = f_builder.AppendRowGroup();
    // Write the metadata
    // rowgroup1 metadata
    auto col1_builder = rg1_builder.NextColumnChunk();
    auto col2_builder = rg1_builder.NextColumnChunk();
    // column metadata
    std::map<Encoding::type, int32_t> dict_encoding_stats({{Encoding::RLE_DICTIONARY, 1}});
    std::map<Encoding::type, int32_t> data_encoding_stats(
        {{Encoding::PLAIN, 1}, {Encoding::RLE, 1}});
    stats_int.set_is_signed(true);
    col1_builder.SetStatistics(stats_int);
    stats_float.set_is_signed(true);
    col2_builder.SetStatistics(stats_float);
    col1_builder.Finish(nrows / 2, 4, 0, 10, 512, 600, true, false, dict_encoding_stats,
        data_encoding_stats);
    col2_builder.Finish(nrows / 2, 24, 0, 30, 512, 600, true, false, dict_encoding_stats,
        data_encoding_stats);

    rg1_builder.set_num_rows(nrows / 2);
    rg1_builder.Finish(1024);

    // rowgroup2 metadata
    auto rg2_builder = f_builder.AppendRowGroup();
    col1_builder = rg2_builder.NextColumnChunk();
    col2_builder = rg2_builder.NextColumnChunk();
    // column metadata
    col1_builder.SetStatistics(stats_int);
    col2_builder.SetStatistics(stats_float);
    dict_encoding_stats.clear();
    col1_builder.Finish(nrows / 2, /*dictionary_page_offset=*/0, 0, 10, 512, 600,
        /*has_dictionary=*/false, false, dict_encoding_stats,
        data_encoding_stats);
    col2_builder.Finish(nrows / 2, 16, 0, 26, 512, 600, true, false, dict_encoding_stats,
        data_encoding_stats);

    rg2_builder.set_num_rows(nrows / 2);
    rg2_builder.Finish(1024);

    // Return the metadata accessor
    return f_builder.Finish();
  }

  @Test
  void buildAccess() {

    List<SchemaNode> fields;
    SchemaNode root;
    SchemaDescriptor schema;

    WriterProperties.Builder prop_builder = new WriterProperties.Builder();
    WriterProperties props = prop_builder
        .version(ParquetVersion.PARQUET_2_6)
        .build();

    fields.add(new SchemaPrimitiveNode("int_col", RepetitionType.REQUIRED, ParquetType.INT32));
    fields.add(new SchemaPrimitiveNode("float_col", RepetitionType.REQUIRED, ParquetType.FLOAT));
    root = new SchemaGroupNode("schema", RepetitionType.REPEATED, fields);
    schema = new SchemaDescriptor(root);

    long nrows = 1000;
    int int_min = 100;
    int int_max = 200;

    EncodedStatistics stats_int;
    stats_int.setNullCount(0)
        .setDistinctCount(nrows)
        .setMin(std::string (reinterpret_cast <const char*>( & int_min),4))
        .setMax(std::string (reinterpret_cast <const char*>( & int_max),4));
    EncodedStatistics stats_float;
    float float_min = 100.100f, float_max = 200.200f;
    stats_float.setNullCount(0)
        .setDistinctCount(nrows)
        .setMin()std::string (reinterpret_cast <const char*>( & float_min),4))
        .setMax(std::string (reinterpret_cast <const char*>( & float_max),4));

    // Generate the metadata
    FileMetaData f_accessor = generateTableMetaData(schema, props, nrows, stats_int, stats_float);

    byte[] f_accessor_serialized_metadata = f_accessor.serializeToByteArray();
    int expected_len = f_accessor_serialized_metadata.length;

    // decoded_len is an in-out parameter
    int decoded_len = expected_len;
    FileMetaData f_accessor_copy = new FileMetaData();
    //int x = f_accessor_copy.
     //   FileMetaData::Make (f_accessor_serialized_metadata.data(), &decoded_len);

    // Check that all of the serialized data is consumed
    assertEquals(expected_len, decoded_len);

    // Run this block twice, one for f_accessor, one for f_accessor_copy.
    // To make sure SerializedMetadata was deserialized correctly.
    FileMetaData[] f_accessors = {f_accessor, f_accessor_copy};
    for (int loop_index = 0; loop_index < 2; loop_index++) {
      // file metadata
      assertEquals(nrows, f_accessors[loop_index].numRows());
      ASSERT_LE(0, static_cast < int>(f_accessors[loop_index].size()));
      assertEquals(2, f_accessors[loop_index].num_row_groups());
      assertEquals(ParquetVersion.PARQUET_2_6, f_accessors[loop_index].version());
      assertEquals(DefaultProperties.DEFAULT_CREATED_BY, f_accessors[loop_index].createdBy());
      assertEquals(3, f_accessors[loop_index].numSchemaElements());

      // row group1 metadata
      RowGroupMetaData rg1_accessor = f_accessors[loop_index].rowGroup(0);
      assertEquals(2, rg1_accessor . numColumns());
      assertEquals(nrows / 2, rg1_accessor . numRows());
      assertEquals(1024, rg1_accessor . totalByteSize());
      assertEquals(1024, rg1_accessor . totalCompressedSize());
      assertEquals(rg1_accessor . fileOffset(),
          rg1_accessor . columnChunk(0).dictionary_page_offset());

      ColumnChunkMetaData rg1_column1 = rg1_accessor . columnChunk(0);
      ColumnChunkMetaData rg1_column2 = rg1_accessor . columnChunk(1);
      assertEquals(true, rg1_column1 . isStatsSet());
      assertEquals(true, rg1_column2 . isStatsSet());
      assertEquals(stats_float.min(), rg1_column2 . statistics().EncodeMin());
      assertEquals(stats_float.max(), rg1_column2 . statistics().EncodeMax());
      assertEquals(stats_int.min(), rg1_column1 . statistics().EncodeMin());
      assertEquals(stats_int.max(), rg1_column1 . statistics().EncodeMax());
      assertEquals(0, rg1_column1 . statistics().null_count());
      assertEquals(0, rg1_column2 . statistics().null_count());
      assertEquals(nrows, rg1_column1 . statistics().distinct_count());
      assertEquals(nrows, rg1_column2 . statistics().distinct_count());
      assertEquals( DEFAULT_COMPRESSION_TYPE, rg1_column1 . compression());
      assertEquals(DEFAULT_COMPRESSION_TYPE, rg1_column2 . compression());
      assertEquals(nrows / 2, rg1_column1 . numValues());
      assertEquals(nrows / 2, rg1_column2 . numValues());
      assertEquals(3, rg1_column1 . encodings().size());
      assertEquals(3, rg1_column2 . encodings().size());
      assertEquals(512, rg1_column1 . total_compressed_size());
      assertEquals(512, rg1_column2 . total_compressed_size());
      assertEquals(600, rg1_column1 . total_uncompressed_size());
      assertEquals(600, rg1_column2 . total_uncompressed_size());
      assertEquals(4, rg1_column1 . dictionary_page_offset());
      assertEquals(24, rg1_column2 . dictionary_page_offset());
      assertEquals(10, rg1_column1 . data_page_offset());
      assertEquals(30, rg1_column2 . data_page_offset());
      assertEquals(3, rg1_column1 . encodingStats().size());
      assertEquals(3, rg1_column2 . encodingStats().size());

      RowGroupMetaData rg2_accessor = f_accessors[loop_index].rowGroup(1);
      assertEquals(2, rg2_accessor . num_columns());
      assertEquals(nrows / 2, rg2_accessor . num_rows());
      assertEquals(1024, rg2_accessor . total_byte_size());
      assertEquals(1024, rg2_accessor . total_compressed_size());
      EXPECT_EQ(rg2_accessor . file_offset(),
          rg2_accessor . ColumnChunk(0).data_page_offset());

      ColumnChunkMetaData rg2_column1 = rg2_accessor . columnChunk(0);
      ColumnChunkMetaData rg2_column2 = rg2_accessor . columnChunk(1);
      assertEquals(true, rg2_column1 . isStatsSet());
      assertEquals(true, rg2_column2 . isStatsSet());
      assertEquals(stats_float.min(), rg2_column2 . statistics().EncodeMin());
      assertEquals(stats_float.max(), rg2_column2 . statistics().EncodeMax());
      assertEquals(stats_int.min(), rg1_column1 . statistics().EncodeMin());
      assertEquals(stats_int.max(), rg1_column1 . statistics().EncodeMax());
      assertEquals(0, rg2_column1 . statistics().null_count());
      assertEquals(0, rg2_column2 . statistics().null_count());
      assertEquals(nrows, rg2_column1 . statistics().distinct_count());
      assertEquals(nrows, rg2_column2 . statistics().distinct_count());
      assertEquals(nrows / 2, rg2_column1 . num_values());
      assertEquals(nrows / 2, rg2_column2 . num_values());
      assertEquals(DEFAULT_COMPRESSION_TYPE, rg2_column1 . compression());
      assertEquals(DEFAULT_COMPRESSION_TYPE, rg2_column2 . compression());
      assertEquals(2, rg2_column1 . encodings().size());
      assertEquals(3, rg2_column2 . encodings().size());
      assertEquals(512, rg2_column1 . total_compressed_size());
      assertEquals(512, rg2_column2 . total_compressed_size());
      assertEquals(600, rg2_column1 . total_uncompressed_size());
      assertEquals(600, rg2_column2 . total_uncompressed_size());
      assertFalse(rg2_column1 . hasDictionaryPage());
      assertEquals(0, rg2_column1 . dictionary_page_offset());
      assertEquals(16, rg2_column2 . dictionary_page_offset());
      assertEquals(10, rg2_column1 . data_page_offset());
      assertEquals(26, rg2_column2 . data_page_offset());
      assertEquals(2, rg2_column1 . encodingStats().size());
      assertEquals(2, rg2_column2 . encodingStats().size());

      // Test FileMetaData::set_file_path
      assertTrue(rg2_column1 . filePath().isEmpty());
      f_accessors[loop_index].setFilePath("/foo/bar/bar.parquet");
      assertEquals("/foo/bar/bar.parquet", rg2_column1 . filePath());
    }

    // Test AppendRowGroups
    auto f_accessor_2 = GenerateTableMetaData(schema, props, nrows, stats_int, stats_float);
    f_accessor . AppendRowGroups( * f_accessor_2);
    assertEquals(4, f_accessor . num_row_groups());
    assertEquals(nrows * 2, f_accessor . num_rows());
    ASSERT_LE(0, static_cast < int>(f_accessor . size()));
    assertEquals(ParquetVersion::PARQUET_2_6, f_accessor . version());
    assertEquals(DEFAULT_CREATED_BY, f_accessor . created_by());
    assertEquals(3, f_accessor . num_schema_elements());

    // Test AppendRowGroups from self (ARROW-13654)
    f_accessor . AppendRowGroups( * f_accessor);
    assertEquals(8, f_accessor . num_row_groups());
    assertEquals(nrows * 4, f_accessor . num_rows());
    assertEquals(3, f_accessor . num_schema_elements());

    // Test Subset
    auto f_accessor_1 = f_accessor . Subset({2, 3});
    ASSERT_TRUE(f_accessor_1 . Equals( * f_accessor_2));

    f_accessor_1 = f_accessor_2 . Subset({0});
    f_accessor_1 . AppendRowGroups( * f_accessor . Subset({0}));
    ASSERT_TRUE(f_accessor_1 . Equals( * f_accessor . Subset({2, 0})));
  }

  @Test
  void v1Version() {

    // PARQUET-839
    parquet::schema::NodeVector fields;
    parquet::schema::NodePtr root;
    parquet::SchemaDescriptor schema;

    WriterProperties::Builder prop_builder;

    std::shared_ptr<WriterProperties> props =
        prop_builder.version(ParquetVersion::PARQUET_1_0).build();

    fields.add(parquet::schema::Int32("int_col", RepetitionType.REQUIRED));
    fields.add(parquet::schema::Float("float_col", RepetitionType.REQUIRED));
    root = parquet::schema::GroupNode::Make("schema", RepetitionType.REPEATED, fields);
    schema.Init(root);

    auto f_builder = FileMetaDataBuilder::Make(&schema, props);

    // Read the metadata
    auto f_accessor = f_builder.Finish();

    // file metadata
    assertEquals(ParquetVersion::PARQUET_1_0, f_accessor.version());
  }

  @Test
  void keyValueMetadata() {

    parquet::schema::NodeVector fields;
    parquet::schema::NodePtr root;
    parquet::SchemaDescriptor schema;

    WriterProperties::Builder prop_builder;

    std::shared_ptr<WriterProperties> props =
        prop_builder.version(ParquetVersion::PARQUET_1_0).build();

    fields.add(parquet::schema::Int32("int_col", RepetitionType.REQUIRED));
    fields.add(parquet::schema::Float("float_col", RepetitionType.REQUIRED));
    root = parquet::schema::GroupNode::Make("schema", RepetitionType.REPEATED, fields);
    schema.Init(root);

    auto kvmeta = std::make_shared<KeyValueMetadata>();
    kvmeta.Append("test_key", "test_value");

    auto f_builder = FileMetaDataBuilder::Make(&schema, props, kvmeta);

    // Read the metadata
    auto f_accessor = f_builder.Finish();

    // Key value metadata
    ASSERT_TRUE(f_accessor.key_value_metadata());
    EXPECT_TRUE(f_accessor.key_value_metadata().Equals(*kvmeta));
  }
}
