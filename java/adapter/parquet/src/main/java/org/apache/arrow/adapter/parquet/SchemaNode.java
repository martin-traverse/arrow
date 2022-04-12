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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.arrow.adapter.parquet.type.ConvertedType;
import org.apache.arrow.adapter.parquet.type.LogicalType;
import org.apache.arrow.adapter.parquet.type.RepetitionType;
import org.apache.parquet.format.SchemaElement;


/**
 * Base class for logical schema types.
 *
 * A type has a name, repetition level, and optionally a logical type (ConvertedType in Parquet metadata parlance).
 */
public abstract class SchemaNode {

  /** Schema nodes can have either primitive or group type. **/
  public enum Type {
    PRIMITIVE,
    GROUP
  }

  protected final SchemaNode.Type nodeType;
  protected final String name;
  protected final RepetitionType repetition;
  protected final LogicalType logicalType;
  protected final ConvertedType convertedType;
  protected final int fieldId;

  // Nodes should not be shared, they have a single parent.
  // Parent is not part of equals() or hashCode()
  private SchemaNode parent;

  protected SchemaNode(
      SchemaNode.Type nodeType, String name,
      RepetitionType repetition,
      LogicalType logicalType,
      ConvertedType convertedType,
      int fieldId /* = -1 */) {

    this.nodeType = nodeType;
    this.name = name;
    this.repetition = repetition;
    this.logicalType = logicalType;
    this.convertedType = convertedType;
    this.fieldId = fieldId;
    this.parent = null;
  }

  public String name() {
    return name;
  }

  public Type nodeType() {
    return nodeType;
  }

  public RepetitionType repetition() {
    return repetition;
  }

  public ConvertedType convertedType() {
    return convertedType;
  }

  public LogicalType logicalType() {
    return logicalType;
  }

  /**
   * The field_id value for the serialized SchemaElement.
   *
   * If the field_id is less than 0 (e.g. -1), it will not be set when serialized to Thrift.
   */
  public int fieldId() {
    return fieldId;
  }

  protected void setParent(SchemaNode parent) {

    this.parent = parent;
  }

  public SchemaNode parent() {
    return parent;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) { return true; }
    if (!(o instanceof SchemaNode)) { return false; }

    SchemaNode that = (SchemaNode) o;

    if (nodeType != that.nodeType) { return false; }
    if (!Objects.equals(name, that.name)) { return false; }
    if (repetition != that.repetition) { return false; }
    if (convertedType != that.convertedType) { return false; }
    if (fieldId != that.fieldId) { return false; }
    return Objects.equals(logicalType, that.logicalType);
  }

  @Override
  public int hashCode() {
    int result = nodeType != null ? nodeType.hashCode() : 0;
    result = 31 * result + (name != null ? name.hashCode() : 0);
    result = 31 * result + (repetition != null ? repetition.hashCode() : 0);
    result = 31 * result + (convertedType != null ? convertedType.hashCode() : 0);
    result = 31 * result + fieldId;
    result = 31 * result + (logicalType != null ? logicalType.hashCode() : 0);
    return result;
  }

  public boolean isPrimitive() {
    return nodeType == Type.PRIMITIVE;
  }

  public boolean isGroup() {
    return nodeType == Type.GROUP;
  }

  public boolean isOptional() {
    return repetition == RepetitionType.OPTIONAL;
  }

  public boolean isRepeated() {
    return repetition == RepetitionType.REPEATED;
  }

  public boolean isRequired() {
    return repetition == RepetitionType.REQUIRED;
  }

  public ColumnPath path() {

    return ColumnPath.fromNode(this);
  }

  /** Produce a Thrift SchemaElement for this schema node. */
  public abstract SchemaElement toParquet();

  /** Convert a group node into a flat list of Thrift schema elements. */
  // In CPP this is implemented with visitors
  public static List<SchemaElement> flatten(SchemaGroupNode group) {

    return flatten(group, new ArrayList<>());
  }

  private static List<SchemaElement> flatten(SchemaNode node, List<SchemaElement> elements) {

    SchemaElement element = node.toParquet();
    elements.add(element);

    if (node.isGroup()) {
      SchemaGroupNode groupNode = (SchemaGroupNode) node;
      for (int i = 0; i < groupNode.fieldCount(); i++) {
        flatten(groupNode.field(i), elements);
      }
    }

    return elements;
  }

  /** Convert a flat list of Thrift schema elements into a structured hierarchy of schema nodes. */
  public static SchemaNode unflatten(List<SchemaElement> elements) {

    if (elements.isEmpty()) {
      throw new ParquetException("Parquet schema contains no elements");
    }

    if (elements.get(0).getNum_children() == 0) {

      if (elements.size() > 1) {
        throw new ParquetException("Parquet schema contains multiple elements but root had no children");
      }

      return SchemaGroupNode.fromParquet(elements.get(0), Collections.emptyList());
    }

    // We don't check that the root node is repeated since this is not
    // consistently set by implementations

    UnflattenElements unflatten = new UnflattenElements(elements, 0);
    return unflattenNextNode(unflatten);
  }

  private static SchemaNode unflattenNextNode(UnflattenElements unflatten) {

    if (unflatten.pos == unflatten.elements.size()) {
      throw new ParquetException("Malformed schema: not enough elements");
    }

    SchemaElement element = unflatten.elements.get(unflatten.pos++);

    if (element.getNum_children() == 0 && element.isSetType()) {

      // Leaf (primitive) node: always has a type
      return SchemaPrimitiveNode.fromParquet(element);

    } else {

      // Group node (may have 0 children, but cannot have a type)
      List<SchemaNode> fields = new ArrayList<>();

      for (int i = 0; i < element.getNum_children(); ++i) {

        SchemaNode field = unflattenNextNode(unflatten);
        fields.add(field);
      }

      return SchemaGroupNode.fromParquet(element, fields);
    }
  }

  private static class UnflattenElements {

    List<SchemaElement> elements;
    int pos;

    UnflattenElements(List<SchemaElement> elements, int pos) {
      this.elements = elements;
      this.pos = pos;
    }
  }
}
