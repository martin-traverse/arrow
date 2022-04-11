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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.arrow.adapter.parquet.ParquetException;
import org.apache.arrow.adapter.parquet.SchemaNode;
import org.apache.arrow.adapter.parquet.SchemaPrimitiveNode;
import org.apache.arrow.adapter.parquet.type.ConvertedType;
import org.apache.arrow.adapter.parquet.type.ParquetType;
import org.apache.arrow.adapter.parquet.type.RepetitionType;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;


public class PrimitiveNodeTest {

  @Test
  void attrs() {

    SchemaPrimitiveNode node1 = new SchemaPrimitiveNode(
        "foo", RepetitionType.REPEATED, ParquetType.INT32);

    SchemaPrimitiveNode node2 = new SchemaPrimitiveNode(
        "bar", RepetitionType.OPTIONAL, ParquetType.BYTE_ARRAY, ConvertedType.UTF8);

    assertEquals("foo", node1.name());
    
    assertTrue(node1.isPrimitive());
    assertFalse(node1.isGroup());

    assertEquals(RepetitionType.REPEATED, node1.repetition());
    assertEquals(RepetitionType.OPTIONAL, node2.repetition());

    assertEquals(SchemaNode.Type.PRIMITIVE, node1.nodeType());

    assertEquals(ParquetType.INT32, node1.physicalType());
    assertEquals(ParquetType.BYTE_ARRAY, node2.physicalType());

    // logical types
    assertEquals(ConvertedType.NONE, node1.convertedType());
    assertEquals(ConvertedType.UTF8, node2.convertedType());

    // repetition
    SchemaPrimitiveNode node3 = new SchemaPrimitiveNode("foo", RepetitionType.REPEATED, ParquetType.INT32);
    SchemaPrimitiveNode node4 = new SchemaPrimitiveNode("foo", RepetitionType.REQUIRED, ParquetType.INT32);
    SchemaPrimitiveNode node5 = new SchemaPrimitiveNode("foo", RepetitionType.OPTIONAL, ParquetType.INT32);

    assertTrue(node3.isRepeated());
    assertFalse(node3.isOptional());

    assertTrue(node4.isRequired());

    assertTrue(node5.isOptional());
    assertFalse(node5.isRequired());
  }

  @Disabled("not implemented yet")
  @Test
  void fromParquet() {

    fail();
  }

  static class MutablePrimitiveNode extends SchemaPrimitiveNode {

    public MutablePrimitiveNode(
        String name, RepetitionType repetition,
        ParquetType physicalType, ConvertedType convertedType,
        int length, int precision, int scale) {

      super(name, repetition, physicalType, convertedType, length, precision, scale, /* fieldId = */ -1);
    }

    public void setTypeLength(int length) {
      typeLength = length;
    }
  }

  @Test
  void equals() {

    SchemaPrimitiveNode node1 = new SchemaPrimitiveNode("foo", RepetitionType.REQUIRED, ParquetType.INT32);
    SchemaPrimitiveNode node2 = new SchemaPrimitiveNode("foo", RepetitionType.REQUIRED, ParquetType.INT64);
    SchemaPrimitiveNode node3 = new SchemaPrimitiveNode("bar", RepetitionType.REQUIRED, ParquetType.INT32);
    SchemaPrimitiveNode node4 = new SchemaPrimitiveNode("foo", RepetitionType.OPTIONAL, ParquetType.INT32);
    SchemaPrimitiveNode node5 = new SchemaPrimitiveNode("foo", RepetitionType.REQUIRED, ParquetType.INT32);

    assertEquals(node1, node1);
    assertNotEquals(node1, node2);
    assertNotEquals(node1, node3);
    assertNotEquals(node1, node4);
    assertEquals(node1, node5);

    SchemaPrimitiveNode flba1 = new SchemaPrimitiveNode(
        "foo", RepetitionType.REQUIRED,
        ParquetType.FIXED_LEN_BYTE_ARRAY, ConvertedType.DECIMAL,
        12, 4, 2);

    MutablePrimitiveNode flba2 = new MutablePrimitiveNode(
        "foo", RepetitionType.REQUIRED,
        ParquetType.FIXED_LEN_BYTE_ARRAY, ConvertedType.DECIMAL,
        1, 4, 2);

    flba2.setTypeLength(12);

    MutablePrimitiveNode flba3 = new MutablePrimitiveNode(
        "foo", RepetitionType.REQUIRED,
        ParquetType.FIXED_LEN_BYTE_ARRAY, ConvertedType.DECIMAL,
        1, 4, 2);

    flba3.setTypeLength(16);

    SchemaPrimitiveNode flba4 = new SchemaPrimitiveNode(
        "foo", RepetitionType.REQUIRED,
        ParquetType.FIXED_LEN_BYTE_ARRAY, ConvertedType.DECIMAL,
        12, 4, 0);

    SchemaPrimitiveNode flba5 = new SchemaPrimitiveNode(
        "foo", RepetitionType.REQUIRED,
        ParquetType.FIXED_LEN_BYTE_ARRAY, ConvertedType.NONE,
        12, 4, 0);

    assertEquals(flba1, flba2);
    assertNotEquals(flba1, flba3);
    assertNotEquals(flba1, flba4);
    assertNotEquals(flba1, flba5);
  }

  @Test
  void physicalLogicalMapping() {

    assertDoesNotThrow(() ->
        new SchemaPrimitiveNode(
            "foo", RepetitionType.REQUIRED, 
            ParquetType.INT32, ConvertedType.INT_32));
    
    assertDoesNotThrow(() -> 
        new SchemaPrimitiveNode(
            "foo", RepetitionType.REQUIRED, 
            ParquetType.BYTE_ARRAY, ConvertedType.JSON));
    
    assertThrows(ParquetException.class, () ->
        new SchemaPrimitiveNode(
            "foo", RepetitionType.REQUIRED,
            ParquetType.INT32, ConvertedType.JSON));

    assertDoesNotThrow(() ->
        new SchemaPrimitiveNode(
            "foo", RepetitionType.REQUIRED,
            ParquetType.INT64, ConvertedType.TIMESTAMP_MILLIS));

    assertThrows(ParquetException.class, () ->
            new SchemaPrimitiveNode(
                "foo", RepetitionType.REQUIRED,
                ParquetType.INT32, ConvertedType.INT_64));

    assertThrows(ParquetException.class, () ->
            new SchemaPrimitiveNode(
                "foo", RepetitionType.REQUIRED,
                ParquetType.BYTE_ARRAY, ConvertedType.INT_8));

    assertThrows(ParquetException.class, () ->
            new SchemaPrimitiveNode(
                "foo", RepetitionType.REQUIRED,
                ParquetType.BYTE_ARRAY, ConvertedType.INTERVAL));

    assertThrows(ParquetException.class, () ->
            new SchemaPrimitiveNode(
                "foo", RepetitionType.REQUIRED,
                ParquetType.FIXED_LEN_BYTE_ARRAY, ConvertedType.ENUM));

    assertDoesNotThrow(() ->
        new SchemaPrimitiveNode(
            "foo", RepetitionType.REQUIRED,
            ParquetType.BYTE_ARRAY, ConvertedType.ENUM));

    assertThrows(ParquetException.class, () -> 
        new SchemaPrimitiveNode(
            "foo", RepetitionType.REQUIRED,
            ParquetType.FIXED_LEN_BYTE_ARRAY, ConvertedType.DECIMAL, 0, 2, 4));

    assertThrows(ParquetException.class, () ->
            new SchemaPrimitiveNode(
                "foo", RepetitionType.REQUIRED,
                ParquetType.FLOAT, ConvertedType.DECIMAL, 0, 2, 4));

    assertThrows(ParquetException.class, () -> 
        new SchemaPrimitiveNode(
            "foo", RepetitionType.REQUIRED,
            ParquetType.FIXED_LEN_BYTE_ARRAY, ConvertedType.DECIMAL, 0, 4, 0));

    assertThrows(ParquetException.class, () -> 
        new SchemaPrimitiveNode(
            "foo", RepetitionType.REQUIRED,
            ParquetType.FIXED_LEN_BYTE_ARRAY, ConvertedType.DECIMAL, 10, 0, 4));

    assertThrows(ParquetException.class, () -> 
        new SchemaPrimitiveNode(
            "foo", RepetitionType.REQUIRED,
            ParquetType.FIXED_LEN_BYTE_ARRAY, ConvertedType.DECIMAL, 10, 4, -1));

    assertThrows(ParquetException.class, () -> 
        new SchemaPrimitiveNode(
            "foo", RepetitionType.REQUIRED,
            ParquetType.FIXED_LEN_BYTE_ARRAY, ConvertedType.DECIMAL, 10, 2, 4));

    assertDoesNotThrow(() ->
        new SchemaPrimitiveNode(
            "foo", RepetitionType.REQUIRED,
            ParquetType.FIXED_LEN_BYTE_ARRAY, ConvertedType.DECIMAL, 10, 6, 4));

    assertDoesNotThrow(() ->
        new SchemaPrimitiveNode(
            "foo", RepetitionType.REQUIRED,
            ParquetType.FIXED_LEN_BYTE_ARRAY, ConvertedType.INTERVAL, 12));

    assertThrows(ParquetException.class, () -> 
        new SchemaPrimitiveNode(
            "foo", RepetitionType.REQUIRED,
            ParquetType.FIXED_LEN_BYTE_ARRAY, ConvertedType.INTERVAL, 10));
  }
}
