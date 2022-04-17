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

package org.apache.arrow.adapter.parquet.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.adapter.parquet.ParquetException;
import org.apache.arrow.adapter.parquet.type.ConvertedType;
import org.apache.arrow.adapter.parquet.type.ParquetType;
import org.apache.arrow.adapter.parquet.type.RepetitionType;
import org.junit.jupiter.api.Test;


public class SchemaDescriptorTest {

  @Test
  void initNonGroup() {

    SchemaNode node = new SchemaPrimitiveNode("field", RepetitionType.OPTIONAL, ParquetType.INT32);

    assertThrows(ParquetException.class, () -> new SchemaDescriptor(node));
  }
  
  @Test
  void equals() {

    SchemaNode inta = new SchemaPrimitiveNode("a", RepetitionType.REQUIRED, ParquetType.INT32);
    SchemaNode intb = new SchemaPrimitiveNode("b", RepetitionType.OPTIONAL, ParquetType.INT64);
    SchemaNode intb2 = new SchemaPrimitiveNode("b2", RepetitionType.OPTIONAL, ParquetType.INT64);
    SchemaNode intc = new SchemaPrimitiveNode("c", RepetitionType.REPEATED, ParquetType.BYTE_ARRAY);

    SchemaNode item1 = new SchemaPrimitiveNode("item1", RepetitionType.REQUIRED, ParquetType.INT64);
    SchemaNode item2 = new SchemaPrimitiveNode("item2", RepetitionType.OPTIONAL, ParquetType.BOOLEAN);
    SchemaNode item3 = new SchemaPrimitiveNode("item3", RepetitionType.REPEATED, ParquetType.INT32);
    SchemaNode list = new SchemaGroupNode("records", RepetitionType.REPEATED, Arrays.asList(item1, item2, item3),
        ConvertedType.LIST);

    SchemaNode bag = new SchemaGroupNode("bag", RepetitionType.OPTIONAL, Collections.singletonList(list));
    SchemaNode bag2 = new SchemaGroupNode("bag", RepetitionType.REQUIRED, Collections.singletonList(list));

    SchemaDescriptor descr1 = new SchemaDescriptor(
        new SchemaGroupNode("schema", RepetitionType.REPEATED, Arrays.asList(inta, intb, intc, bag)));

    assertEquals(descr1, descr1);

    SchemaDescriptor descr2 = new SchemaDescriptor(
        new SchemaGroupNode("schema", RepetitionType.REPEATED, Arrays.asList(inta, intb, intc, bag2)));

    assertNotEquals(descr1, descr2);

    SchemaDescriptor descr3 = new SchemaDescriptor(
        new SchemaGroupNode("schema", RepetitionType.REPEATED, Arrays.asList(inta, intb2, intc, bag)));

    assertNotEquals(descr1, descr3);

    // Robust to name of parent node
    SchemaDescriptor descr4 = new SchemaDescriptor(
        new SchemaGroupNode("SCHEMA", RepetitionType.REPEATED, Arrays.asList(inta, intb, intc, bag)));

    assertEquals(descr1, descr4);

    SchemaDescriptor descr5 = new SchemaDescriptor(
        new SchemaGroupNode("schema", RepetitionType.REPEATED, Arrays.asList(inta, intb, intc, bag, intb2)));

    assertNotEquals(descr1, descr5);

    // Different max repetition / definition levels
    ColumnDescriptor col1 = new ColumnDescriptor(inta, (short) 5, (short) 1);
    ColumnDescriptor col2 = new ColumnDescriptor(inta, (short) 6, (short) 1);
    ColumnDescriptor col3 = new ColumnDescriptor(inta, (short) 5, (short) 2);

    assertEquals(col1, col1);
    assertNotEquals(col1, col2);
    assertNotEquals(col1, col3);
  }

  @Test
  void buildTree() {

    List<SchemaNode> fields = new ArrayList<>();

    SchemaNode inta = new SchemaPrimitiveNode("a", RepetitionType.REQUIRED, ParquetType.INT32);
    fields.add(inta);
    fields.add(new SchemaPrimitiveNode("b", RepetitionType.OPTIONAL, ParquetType.INT64));
    fields.add(new SchemaPrimitiveNode("c", RepetitionType.REPEATED, ParquetType.BYTE_ARRAY));

    // 3-level list encoding
    SchemaNode item1 = new SchemaPrimitiveNode("item1", RepetitionType.REQUIRED, ParquetType.INT64);
    SchemaNode item2 = new SchemaPrimitiveNode("item2", RepetitionType.OPTIONAL, ParquetType.BOOLEAN);
    SchemaNode item3 = new SchemaPrimitiveNode("item3", RepetitionType.REPEATED, ParquetType.INT32);
    SchemaNode list = new SchemaGroupNode("records", RepetitionType.REPEATED, 
        Arrays.asList(item1, item2, item3), ConvertedType.LIST);
    
    SchemaNode bag = new SchemaGroupNode("bag", RepetitionType.OPTIONAL, Collections.singletonList(list));
    fields.add(bag);

    SchemaNode schema = new SchemaGroupNode("schema", RepetitionType.REPEATED, fields);
    SchemaDescriptor descr = new SchemaDescriptor(schema);
    
    //                             mdef mrep
    // required int32 a            0    0
    // optional int64 b            1    0
    // repeated byte_array c       1    1
    // optional group bag          1    0
    //   repeated group records    2    1
    //     required int64 item1    2    1
    //     optional boolean item2  3    1
    //     repeated int32 item3    3    2
    int nLeaves = 6;
    long[] exMaxDefLevels = new long[] {0, 1, 1, 2, 3, 3};
    long[] exMaxRepLevels = new long[] {0, 0, 1, 1, 1, 2};

    // 6 leaves
    assertEquals(nLeaves, descr.numColumns());

    for (int i = 0; i < nLeaves; ++i) {
      ColumnDescriptor col = descr.column(i);
      assertEquals(exMaxDefLevels[i], col.maxDefinitionLevel());
      assertEquals(exMaxRepLevels[i], col.maxRepetitionLevel());
    }

    assertEquals(descr.column(0).path().toDotString(), "a");
    assertEquals(descr.column(1).path().toDotString(), "b");
    assertEquals(descr.column(2).path().toDotString(), "c");
    assertEquals(descr.column(3).path().toDotString(), "bag.records.item1");
    assertEquals(descr.column(4).path().toDotString(), "bag.records.item2");
    assertEquals(descr.column(5).path().toDotString(), "bag.records.item3");

    for (int i = 0; i < nLeaves; ++i) {
      ColumnDescriptor col = descr.column(i);
      assertEquals(i, descr.columnIndex(col.schemaNode()));
    }

    // Test non-column nodes find
    SchemaNode nonColumnAlien = new SchemaPrimitiveNode("alien", RepetitionType.REQUIRED, ParquetType.INT32);
    SchemaNode nonColumnFamiliar = new SchemaPrimitiveNode("a", RepetitionType.REPEATED, ParquetType.INT32);
    assertTrue(descr.columnIndex(nonColumnAlien) < 0);
    assertTrue(descr.columnIndex(nonColumnFamiliar) < 0);

    assertEquals(inta, descr.getColumnRoot(0));
    assertEquals(bag, descr.getColumnRoot(3));
    assertEquals(bag, descr.getColumnRoot(4));
    assertEquals(bag, descr.getColumnRoot(5));

    assertEquals(schema, descr.groupNode());

    // Init clears the leaves
    // In the Java implementation schemas are treated as immutable so there is no way to "reset" a schema

    descr = new SchemaDescriptor(schema);
    assertEquals(nLeaves, descr.numColumns());
  }

  @Test
  void hasRepeatedFields() {

    List<SchemaNode> fields = new ArrayList<>();

    SchemaNode inta = new SchemaPrimitiveNode("a", RepetitionType.REQUIRED, ParquetType.INT32);
    fields.add(inta);
    fields.add(new SchemaPrimitiveNode("b", RepetitionType.OPTIONAL, ParquetType.INT64));
    fields.add(new SchemaPrimitiveNode("c", RepetitionType.REPEATED, ParquetType.BYTE_ARRAY));

    SchemaNode schema = new SchemaGroupNode("schema", RepetitionType.REPEATED, fields);
    SchemaDescriptor descr = new SchemaDescriptor(schema);

    assertTrue(descr.hasRepeatedFields());

    // 3-level list encoding
    SchemaNode item1 = new SchemaPrimitiveNode("item1", RepetitionType.REQUIRED, ParquetType.INT64);
    SchemaNode item2 = new SchemaPrimitiveNode("item2", RepetitionType.OPTIONAL, ParquetType.BOOLEAN);
    SchemaNode item3 = new SchemaPrimitiveNode("item3", RepetitionType.REPEATED, ParquetType.INT32);
    SchemaNode list = new SchemaGroupNode("records", RepetitionType.REPEATED,
        Arrays.asList(item1, item2, item3), ConvertedType.LIST);
    SchemaNode bag = new SchemaGroupNode("bag", RepetitionType.OPTIONAL, Collections.singletonList(list));
    fields.add(bag);

    schema = new SchemaGroupNode("schema", RepetitionType.REPEATED, fields);
    descr = new SchemaDescriptor(schema);

    assertTrue(descr.hasRepeatedFields());

    // 3-level list encoding
    SchemaNode itemKey = new SchemaPrimitiveNode("key", RepetitionType.REQUIRED, ParquetType.INT64);
    SchemaNode itemValue = new SchemaPrimitiveNode("value", RepetitionType.OPTIONAL, ParquetType.BOOLEAN);
    SchemaNode map = new SchemaGroupNode("map", RepetitionType.REPEATED,
        Arrays.asList(itemKey, itemValue), ConvertedType.MAP);
    SchemaNode map2 = new SchemaGroupNode("my_map", RepetitionType.OPTIONAL, Collections.singletonList(map));
    fields.add(map2);

    schema = new SchemaGroupNode("schema", RepetitionType.REPEATED, fields);
    descr = new SchemaDescriptor(schema);

    assertTrue(descr.hasRepeatedFields());
    assertTrue(descr.hasRepeatedFields());
  }
}
