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

import static org.apache.arrow.adapter.parquet.SchemaTestHelpers.newGroup;
import static org.apache.arrow.adapter.parquet.SchemaTestHelpers.newPrimitive;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.adapter.parquet.type.ConvertedType;
import org.apache.arrow.adapter.parquet.type.DecimalLogicalType;
import org.apache.arrow.adapter.parquet.type.ListLogicalType;
import org.apache.arrow.adapter.parquet.type.LogicalType;
import org.apache.arrow.adapter.parquet.type.ParquetType;
import org.apache.arrow.adapter.parquet.type.RepetitionType;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Type;
import org.junit.jupiter.api.Test;


public class SchemaFlattenTest {

  String name_ = "parquet_schema";

  @Test
  void decimalMetadata() {

    // Checks that DecimalMetadata is only set for DecimalTypes
    SchemaNode node = new SchemaPrimitiveNode(
        "decimal", RepetitionType.REQUIRED, ParquetType.INT64,
        ConvertedType.DECIMAL, -1, 8, 4);

    SchemaGroupNode group = new SchemaGroupNode(
        "group", RepetitionType.REPEATED, Collections.singletonList(node),
        ConvertedType.LIST);

    List<SchemaElement> elements = SchemaNode.flatten(group);

    assertEquals("decimal", elements.get(1).getName());
    assertTrue(elements.get(1).isSetPrecision());
    assertTrue(elements.get(1).isSetScale());

    // ... including those created with new logical types
    node = new SchemaPrimitiveNode(
        "decimal", RepetitionType.REQUIRED,
        new DecimalLogicalType(10, 5), ParquetType.INT64, -1);

    group = new SchemaGroupNode(
        "group", RepetitionType.REPEATED, Collections.singletonList(node),
        new ListLogicalType());

    elements = SchemaNode.flatten(group);
    assertEquals("decimal", elements.get(1).getName());
    assertTrue(elements.get(1).isSetPrecision());
    assertTrue(elements.get(1).isSetScale());

    // Not for integers with no logical type
    group = new SchemaGroupNode(
        "group", RepetitionType.REPEATED,
        Collections.singletonList(new SchemaPrimitiveNode("int64", RepetitionType.OPTIONAL, ParquetType.INT64)),
        ConvertedType.LIST);

    elements = SchemaNode.flatten(group);
    assertEquals("int64", elements.get(1).name);
    assertFalse(elements.get(0).isSetPrecision());
    assertFalse(elements.get(0).isSetScale());
  }

  @Test
  void nestedExample() {

    SchemaElement elt;
    List<SchemaElement> elements = new ArrayList<>();
    elements.add(newGroup(name_, FieldRepetitionType.REPEATED, 2, 0));

    // A primitive one
    elements.add(newPrimitive("a", FieldRepetitionType.REQUIRED, Type.INT32, 1));

    // A group
    elements.add(newGroup("bag", FieldRepetitionType.OPTIONAL, 1, 2));

    // 3-level list encoding, by hand
    elt = newGroup("b", FieldRepetitionType.REPEATED, 1, 3);
    elt.setConverted_type(org.apache.parquet.format.ConvertedType.LIST);
    org.apache.parquet.format.ListType ls = new org.apache.parquet.format.ListType();
    org.apache.parquet.format.LogicalType lt = new org.apache.parquet.format.LogicalType();
    lt.setLIST(ls);
    elt.setLogicalType(lt);
    elements.add(elt);
    elements.add(newPrimitive("item", FieldRepetitionType.OPTIONAL, Type.INT64, 4));

    // Construct the schema
    List<SchemaNode> fields = new ArrayList<>();
    fields.add(new SchemaPrimitiveNode("a", RepetitionType.REQUIRED, ParquetType.INT32, 1));
    // 3-level list encoding
    SchemaNode item = new SchemaPrimitiveNode("item", RepetitionType.OPTIONAL, ParquetType.INT64, 4);
    SchemaNode list = new SchemaGroupNode(
        "b", RepetitionType.REPEATED, Collections.singletonList(item),
        ConvertedType.LIST, 3);
    SchemaNode bag = new SchemaGroupNode(
        "bag", RepetitionType.OPTIONAL, Collections.singletonList(list),
        (LogicalType) null, 2);
    fields.add(bag);

    SchemaGroupNode schema = new SchemaGroupNode(name_, RepetitionType.REPEATED, fields, (LogicalType) null, 0);

    List<SchemaElement> flattenedElements = SchemaNode.flatten(schema);
    assertEquals(elements.size(), flattenedElements.size());
    for (int i = 0; i < elements.size(); i++) {
      assertEquals(elements.get(i), flattenedElements.get(i));
    }
  }
}
