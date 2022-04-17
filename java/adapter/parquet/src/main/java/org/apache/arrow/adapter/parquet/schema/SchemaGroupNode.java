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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.adapter.parquet.ParquetException;
import org.apache.arrow.adapter.parquet.type.ConvertedType;
import org.apache.arrow.adapter.parquet.type.DecimalMetadata;
import org.apache.arrow.adapter.parquet.type.LogicalType;
import org.apache.arrow.adapter.parquet.type.NoLogicalType;
import org.apache.arrow.adapter.parquet.type.RepetitionType;
import org.apache.parquet.format.SchemaElement;


/**
 * A schema group node holds an ordered collection of fields (i.e. child nodes).
 *
 * Child nodes can be primitives or other group nodes.
 */
public class SchemaGroupNode extends SchemaNode {

  private final List<SchemaNode> fields;
  private final Map<String, List<Integer>> fieldNameToIdx;

  /** Create a schema group node from a physical type only. */
  public SchemaGroupNode(String name, RepetitionType repetition, List<SchemaNode> fields) {

    this(name, repetition, fields, new NoLogicalType(), -1);
  }

  /** Create a schema group node from a legacy converted type. */
  public SchemaGroupNode(
      String name, RepetitionType repetition, List<SchemaNode> fields,
      ConvertedType convertedType) {

    this(name, repetition, fields, convertedType, -1);
  }

  /** Create a schema group node from a legacy converted type. */
  public SchemaGroupNode(
      String name, RepetitionType repetition, List<SchemaNode> fields,
      ConvertedType convertedType, int fieldId) {

    super(
        Type.GROUP, name, repetition,
        LogicalType.fromConvertedType(convertedType, DecimalMetadata.zeroUnset()),
        convertedType,
        fieldId);

    checkAssignedTypes();

    Map<String, List<Integer>> fieldNameToIdx = new HashMap<>();
    prepareFields(fields, fieldNameToIdx);

    this.fields = Collections.unmodifiableList(fields);
    this.fieldNameToIdx = Collections.unmodifiableMap(fieldNameToIdx);
  }

  /** Create a schema group node from a logical type. */
  public SchemaGroupNode(
      String name, RepetitionType repetition, List<SchemaNode> fields,
      LogicalType logicalType) {

    this(name, repetition, fields, logicalType, -1);
  }

  /** Create a schema group node from a logical type. */
  public SchemaGroupNode(
      String name, RepetitionType repetition, List<SchemaNode> fields,
      LogicalType logicalType, int fieldId) {

    super(
        Type.GROUP, name, repetition,
        logicalType != null ? logicalType : new NoLogicalType(),
        logicalType != null ? logicalType.toConvertedType() : ConvertedType.NONE,
        fieldId);

    checkAssignedTypes();

    Map<String, List<Integer>> fieldNameToIdx = new HashMap<>();
    prepareFields(fields, fieldNameToIdx);

    this.fields = Collections.unmodifiableList(fields);
    this.fieldNameToIdx = Collections.unmodifiableMap(fieldNameToIdx);
  }

  private void prepareFields(List<SchemaNode> fields, Map<String, List<Integer>> fieldNameToIdx) {

    int fieldIndex = 0;

    for (SchemaNode field : fields) {

      field.setParent(this);

      if (!fieldNameToIdx.containsKey(field.name)) {
        fieldNameToIdx.put(field.name, new ArrayList<>());
      }

      fieldNameToIdx.get(field.name).add(fieldIndex++);
    }

    for (Map.Entry<String, List<Integer>> entry : fieldNameToIdx.entrySet()) {
      entry.setValue(Collections.unmodifiableList(entry.getValue()));
    }
  }

  private void checkAssignedTypes() {

    if (logicalType == null) {
      throw new ParquetException("Invalid logical type: (null)");
    }

    if (!(logicalType.isNested() || logicalType.isNone())) {
      throw new ParquetException("Logical type " + logicalType + " can not be applied to group node");
    }

    if (!logicalType.isCompatible(convertedType, DecimalMetadata.zeroUnset())) {
      throw new ParquetException(
          "Logical type " + logicalType +
          " is not compatible with converted type " + convertedType);
    }
  }

  public int fieldCount() {
    return fields.size();
  }

  public SchemaNode field(int fieldIndex) {
    return fields.get(fieldIndex);
  }

  /**
   * Get the index of a field by its name, or negative value if not found.
   *
   * If several fields share the same name, it is unspecified which one is returned.
   */
  public int fieldIndex(String name) {

    List<Integer> fields = fieldNameToIdx.get(name);

    if (fields != null) {
      return fields.get(0);
    } else {
      return -1;
    }
  }

  /** Get the index of a field by its node, or negative value if not found. */
  public int fieldIndex(SchemaNode node) {

    List<Integer> indices = fieldNameToIdx.get(node.name);

    if (indices != null) {
      for (int idx : indices) {
        if (fields.get(idx).equals(node)) {
          return idx;
        }
      }
    }

    return -1;
  }

  /** Return true if this node or any child node has REPEATED repetition type. */
  public boolean hasRepeatedFields() {

    for (int i = 0; i < this.fieldCount(); ++i) {

      SchemaNode field = this.field(i);

      if (field.repetition() == RepetitionType.REPEATED) {
        return true;
      }

      // TODO: Check difference with CPP implementation
      // CPP does not continue after checking the first group field
      // Is this a bug?

      if (field.isGroup()) {
        SchemaGroupNode group = (SchemaGroupNode) field;
        if (group.hasRepeatedFields()) {
          return true;
        }
      }
    }

    return false;
  }

  @Override
  public boolean equals(Object o) {

    if (this == o) { return true; }
    if (!(o instanceof SchemaGroupNode)) { return false; }
    if (!super.equals(o)) { return false; }

    SchemaGroupNode that = (SchemaGroupNode) o;

    // Null fields will error in the constructor

    return fields.equals(that.fields);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + fields.hashCode();
    return result;
  }

  /** Create a group schema node from a Thrift schema element and a list of child nodes. */
  public static SchemaGroupNode fromParquet(SchemaElement element, List<SchemaNode> fields) {

    int fieldId = element.isSetField_id() ? element.getField_id() : -1;

    if (element.isSetLogicalType()) {

      // updated writer with logical type present

      return new SchemaGroupNode(
          element.getName(),
          convertEnum(RepetitionType.class, element.getRepetition_type()),
          fields,
          LogicalType.fromThrift(element.getLogicalType()),
          fieldId);

    } else if (element.isSetConverted_type()) {

      // Legacy converted type available

      return new SchemaGroupNode(
          element.getName(),
          convertEnum(RepetitionType.class, element.getRepetition_type()),
          fields,
          convertEnum(ConvertedType.class, element.getConverted_type()),
          fieldId);

    } else {

      // No logical type available, so use NoLogicalType

      return new SchemaGroupNode(
          element.getName(),
          convertEnum(RepetitionType.class, element.getRepetition_type()),
          fields,
          new NoLogicalType(),
          fieldId);
    }
  }

  @Override
  public org.apache.parquet.format.SchemaElement toParquet() {

    org.apache.parquet.format.SchemaElement element = new org.apache.parquet.format.SchemaElement();
    element.setName(name);
    element.setNum_children(fieldCount());
    element.setRepetition_type(convertEnum(org.apache.parquet.format.FieldRepetitionType.class, repetition));

    if (logicalType != null && logicalType.isSerialized()) {
      element.setLogicalType(logicalType.toThrift());
    }

    if (convertedType != ConvertedType.NONE) {
      element.setConverted_type(convertEnum(org.apache.parquet.format.ConvertedType.class, convertedType));
    }

    if (fieldId >= 0) {
      element.setField_id(fieldId);
    }

    return element;
  }

  private static <T extends Enum<T>, S extends Enum<S>> T convertEnum(Class<T> enumClass, S thriftEnum) {

    return Enum.valueOf(enumClass, thriftEnum.name());
  }
}
