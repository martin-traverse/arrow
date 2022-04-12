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

import java.util.Objects;

import org.apache.arrow.adapter.parquet.type.ConvertedType;
import org.apache.arrow.adapter.parquet.type.LogicalType;
import org.apache.arrow.adapter.parquet.type.RepetitionType;


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
  protected ConvertedType convertedType;
  protected LogicalType logicalType;
  protected final int fieldId;

  // Nodes should not be shared, they have a single parent.
  private SchemaNode parent;

  protected SchemaNode(
      SchemaNode.Type nodeType, String name,
      RepetitionType repetition,
      ConvertedType convertedType, /* = ConvertedType::NONE,*/
      int fieldId /* = -1*/) {

    this.nodeType = nodeType;
    this.name = name;
    this.repetition = repetition;
    this.convertedType = convertedType;
    this.logicalType = null; // or LogicalType.none();
    this.fieldId = fieldId;
    this.parent = null;
  }

  protected SchemaNode(
      SchemaNode.Type nodeType, String name,
      RepetitionType repetition,
      LogicalType logicalType,
      int fieldId /* = -1 */) {

    this.nodeType = nodeType;
    this.name = name;
    this.repetition = repetition;
    this.convertedType = ConvertedType.NONE;
    this.logicalType = logicalType;
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

  public abstract org.apache.parquet.format.SchemaElement toParquet();

  protected boolean equalsInternal(SchemaNode other) {

    boolean nameEqual = (name == null && other.name == null) ||
        (name != null && name.equals(other.name));

    boolean logicalTypeEqual = (logicalType == null && other.logicalType == null) ||
        (logicalType != null && other.logicalType != null && logicalType.equals(other.logicalType));

    return nodeType == other.nodeType &&
        nameEqual &&
        repetition == other.repetition &&
        convertedType == other.convertedType &&
        fieldId == other.fieldId() &&
        logicalTypeEqual;
  }

  protected void setParent(SchemaNode parent) {

    this.parent = parent;
  }
}
