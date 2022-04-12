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

import static com.apache.arrow.adapter.parquet.SchemaTestHelpers.newGroup;
import static com.apache.arrow.adapter.parquet.SchemaTestHelpers.newPrimitive;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.adapter.parquet.ParquetException;
import org.apache.arrow.adapter.parquet.SchemaGroupNode;
import org.apache.arrow.adapter.parquet.SchemaNode;
import org.apache.arrow.adapter.parquet.SchemaPrimitiveNode;
import org.apache.arrow.adapter.parquet.type.ConvertedType;
import org.apache.arrow.adapter.parquet.type.LogicalType;
import org.apache.arrow.adapter.parquet.type.ParquetType;
import org.apache.arrow.adapter.parquet.type.RepetitionType;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Type;
import org.junit.jupiter.api.Test;


public class SchemaConverterTest {

  String name = "parquet_schema";

  @Test
  void nestedExample() {

    SchemaElement elt;
    List<SchemaElement> elements = new ArrayList<>();
    elements.add(newGroup(name, FieldRepetitionType.REPEATED, /*num_children=*/ 2, /*field_id=*/ 0));

    // A primitive one
    elements.add(newPrimitive("a", FieldRepetitionType.REQUIRED, Type.INT32, 1));

    // A group
    elements.add(newGroup("bag", FieldRepetitionType.OPTIONAL, 1, 2));

    // 3-level list encoding, by hand
    elt = newGroup("b", FieldRepetitionType.REPEATED, 1, 3);
    elt.setConverted_type(org.apache.parquet.format.ConvertedType.LIST);
    elements.add(elt);
    elements.add(newPrimitive("item", FieldRepetitionType.OPTIONAL, Type.INT64, 4));

    SchemaGroupNode group = unflattenGroup(elements);

    // Construct the expected schema
    List<SchemaNode> fields = new ArrayList<>();
    fields.add(new SchemaPrimitiveNode("a", RepetitionType.REQUIRED, ParquetType.INT32, 1));

    // 3-level list encoding
    SchemaNode item = new SchemaPrimitiveNode("item", RepetitionType.OPTIONAL, ParquetType.INT64, 4);
    SchemaNode list = new SchemaGroupNode("b", RepetitionType.REPEATED,
        Collections.singletonList(item), ConvertedType.LIST, 3);
    SchemaNode bag = new SchemaGroupNode("bag", RepetitionType.OPTIONAL,
        Collections.singletonList(list), (LogicalType) null, 2);
    fields.add(bag);

    SchemaNode schema = new SchemaGroupNode(name, RepetitionType.REPEATED, fields, (LogicalType) null, 0);

    assertEquals(schema, group);

    // Check that the parent relationship in each node is consistent
    assertNull(group.parent());

    assertTrue(checkForParentConsistency(group));
  }

  @Test
  void zeroColumns() {

    // ARROW-3843

    List<SchemaElement> elements = Collections.singletonList(newGroup("schema", FieldRepetitionType.REPEATED, 0, 0));

    assertDoesNotThrow(() -> unflattenGroup(elements));
  }

  @Test
  void invalidRoot() {

    // According to the Parquet specification, the first element in the
    // list<SchemaElement> is a group whose children (and their descendants)
    // contain all of the rest of the flattened schema elements. If the first
    // element is not a group, it is a malformed Parquet file.

    // Java note - we need a list of two elements, in CPP the vector is populated with default values automatically

    List<SchemaElement> elements = new ArrayList<>();
    elements.add(0, newPrimitive("not-a-group", FieldRepetitionType.REQUIRED, Type.INT32, 0));
    elements.add(1, new SchemaElement());
    assertThrows(ParquetException.class, () -> unflattenGroup(elements));

    // While the Parquet spec indicates that the root group should have REPEATED
    // repetition type, some implementations may return REQUIRED or OPTIONAL
    // groups as the first element. These tests check that this is okay as a
    // practicality matter.
    elements.clear();
    elements.add(0, newGroup("not-repeated", FieldRepetitionType.REQUIRED, 1, 0));
    elements.add(1, newPrimitive("a", FieldRepetitionType.REQUIRED, Type.INT32, 1));
    assertDoesNotThrow(() -> unflattenGroup(elements));

    elements.remove(0);
    elements.add(0, newGroup("not-repeated", FieldRepetitionType.OPTIONAL, 1, 0));
    assertDoesNotThrow(() -> unflattenGroup(elements));
  }

  @Test
  void notEnoughChildren() {

    // Throw a ParquetException, but don't core dump or anything
    List<SchemaElement> elements = new ArrayList<>();
    elements.add(newGroup(name, FieldRepetitionType.REPEATED, 2, 0));
    assertThrows(ParquetException.class, () -> unflattenGroup(elements));
  }

  private SchemaGroupNode unflattenGroup(List<SchemaElement> elements) {

    SchemaNode node = SchemaNode.unflatten(elements);
    assertTrue(node.isGroup());
    return (SchemaGroupNode) node;
  }

  private boolean checkForParentConsistency(SchemaGroupNode node) {

    // Each node should have the group as parent
    for (int i = 0; i < node.fieldCount(); i++) {

      SchemaNode field = node.field(i);
      if (field.parent() != node) {
        return false;
      }

      if (field.isGroup()) {
        if (!checkForParentConsistency((SchemaGroupNode) field)) {
          return false;
        }
      }

    }

    return true;
  }
}
