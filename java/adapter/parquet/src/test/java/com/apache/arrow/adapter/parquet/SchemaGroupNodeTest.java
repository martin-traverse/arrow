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

package com.apache.arrow.adapter.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.adapter.parquet.SchemaGroupNode;
import org.apache.arrow.adapter.parquet.SchemaNode;
import org.apache.arrow.adapter.parquet.SchemaPrimitiveNode;
import org.apache.arrow.adapter.parquet.type.ConvertedType;
import org.apache.arrow.adapter.parquet.type.ParquetType;
import org.apache.arrow.adapter.parquet.type.RepetitionType;
import org.junit.jupiter.api.Test;


public class SchemaGroupNodeTest {

  List<SchemaNode> fields1() {

    return Arrays.asList(
        new SchemaPrimitiveNode("one", RepetitionType.REQUIRED, ParquetType.INT32),
        new SchemaPrimitiveNode("two", RepetitionType.OPTIONAL, ParquetType.INT64),
        new SchemaPrimitiveNode("three", RepetitionType.OPTIONAL, ParquetType.DOUBLE));
  }

  List<SchemaNode> fields2() {

    return Arrays.asList(
        new SchemaPrimitiveNode("duplicate", RepetitionType.REQUIRED, ParquetType.INT32),
        new SchemaPrimitiveNode("unique", RepetitionType.OPTIONAL, ParquetType.INT64),
        new SchemaPrimitiveNode("duplicate", RepetitionType.OPTIONAL, ParquetType.DOUBLE));
  }

  @Test
  void attrs() {

    List<SchemaNode> fields = fields1();

    SchemaGroupNode node1 = new SchemaGroupNode("foo", RepetitionType.REPEATED, fields);
    SchemaGroupNode node2 = new SchemaGroupNode("bar", RepetitionType.OPTIONAL, fields, ConvertedType.LIST);

    assertEquals("foo", node1.name());

    assertTrue(node1.isGroup());
    assertFalse(node1.isPrimitive());

    assertEquals(fields.size(), node1.fieldCount());

    assertTrue(node1.isRepeated());
    assertTrue(node2.isOptional());

    assertEquals(RepetitionType.REPEATED, node1.repetition());
    assertEquals(RepetitionType.OPTIONAL, node2.repetition());

    assertEquals(SchemaNode.Type.GROUP, node1.nodeType());

    // logical types
    assertEquals(ConvertedType.NONE, node1.convertedType());
    assertEquals(ConvertedType.LIST, node2.convertedType());
    assertFalse(node1.logicalType().isList());
    assertTrue(node2.logicalType().isList());
  }

  @Test
  void equals() {

    List<SchemaNode> f1 = fields1();
    List<SchemaNode> f2 = fields1();

    SchemaGroupNode group1 = new SchemaGroupNode("group", RepetitionType.REPEATED, f1);
    SchemaGroupNode group2 = new SchemaGroupNode("group", RepetitionType.REPEATED, f2);
    SchemaGroupNode group3 = new SchemaGroupNode("group2", RepetitionType.REPEATED, f2);

    // This is copied in the GroupNode ctor, so this is okay
    f2 = new ArrayList<>(f2);
    f2.add(new SchemaPrimitiveNode("four", RepetitionType.OPTIONAL, ParquetType.FLOAT));

    SchemaGroupNode group4 = new SchemaGroupNode("group", RepetitionType.REPEATED, f2);
    SchemaGroupNode group5 = new SchemaGroupNode("group", RepetitionType.REPEATED, fields1());

    assertEquals(group1, group1);
    assertEquals(group1, group2);
    assertNotEquals(group1, group3);

    assertNotEquals(group1, group4);
    assertNotEquals(group5, group4);
  }

  @Test
  void fieldIndex() {

    List<SchemaNode> fields = fields1();
    SchemaGroupNode group = new SchemaGroupNode("group", RepetitionType.REQUIRED, fields);
    
    for (int i = 0; i < fields.size(); i++) {
      SchemaNode field = group.field(i);
      assertEquals(i, group.fieldIndex(field));
    }

    // Test a non field node
    SchemaPrimitiveNode nonFieldAlien = new SchemaPrimitiveNode(
        "alien", RepetitionType.REQUIRED, ParquetType.INT32); // other name
    SchemaPrimitiveNode nonFieldFamiliar = new SchemaPrimitiveNode(
        "one", RepetitionType.REPEATED, ParquetType.INT32); // other node

    assertTrue(group.fieldIndex(nonFieldAlien) < 0);
    assertTrue(group.fieldIndex(nonFieldFamiliar) < 0);
  }

  @Test
  void fieldIndexDuplicateName() {

    List<SchemaNode> fields = fields2();
    SchemaGroupNode group = new SchemaGroupNode("group", RepetitionType.REQUIRED, fields);
    
    for (int i = 0; i < fields.size(); i++) {
      SchemaNode field = group.field(i);
      assertEquals(i, group.fieldIndex(field));
    }
  }
}
