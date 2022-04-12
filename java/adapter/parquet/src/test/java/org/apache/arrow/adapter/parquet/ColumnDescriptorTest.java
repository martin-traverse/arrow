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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.arrow.adapter.parquet.type.ConvertedType;
import org.apache.arrow.adapter.parquet.type.ParquetType;
import org.apache.arrow.adapter.parquet.type.RepetitionType;
import org.junit.jupiter.api.Test;


public class ColumnDescriptorTest {

  @Test
  void attrs() {

    SchemaNode node = new SchemaPrimitiveNode(
        "name", RepetitionType.OPTIONAL, ParquetType.BYTE_ARRAY,
        ConvertedType.UTF8);

    ColumnDescriptor descr = new ColumnDescriptor(node, (short) 4, (short) 1);

    assertEquals("name", descr.name());
    assertEquals(4, descr.maxDefinitionLevel());
    assertEquals(1, descr.maxRepetitionLevel());

    assertEquals(ParquetType.BYTE_ARRAY, descr.physicalType());

    assertEquals(-1, descr.typeLength());
    
    String expectedDescr = "column descriptor = {\n" +
        "  name: name,\n" +
        "  path: ,\n" +
        "  physical_type: BYTE_ARRAY,\n" +
        "  converted_type: UTF8,\n" +
        "  logical_type: String,\n" +
        "  max_definition_level: 4,\n" +
        "  max_repetition_level: 1,\n" +
        "}";
    
    assertEquals(expectedDescr, descr.toString());

    // Test FIXED_LEN_BYTE_ARRAY
    node = new SchemaPrimitiveNode(
        "name", RepetitionType.OPTIONAL, ParquetType.FIXED_LEN_BYTE_ARRAY,
        ConvertedType.DECIMAL, 12, 10, 4);

    ColumnDescriptor descr2 = new ColumnDescriptor(node, (short) 4, (short) 1);

    assertEquals(ParquetType.FIXED_LEN_BYTE_ARRAY, descr2.physicalType());
    assertEquals(12, descr2.typeLength());

    expectedDescr = "column descriptor = {\n" +
        "  name: name,\n" +
        "  path: ,\n" +
        "  physical_type: FIXED_LEN_BYTE_ARRAY,\n" +
        "  converted_type: DECIMAL,\n" +
        "  logical_type: Decimal(precision=10, scale=4),\n" +
        "  max_definition_level: 4,\n" +
        "  max_repetition_level: 1,\n" +
        "  length: 12,\n" +
        "  precision: 10,\n" +
        "  scale: 4,\n" +
        "}";

    assertEquals(expectedDescr, descr2.toString());

  }
}
