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


import org.apache.arrow.adapter.parquet.type.ConvertedType;
import org.apache.arrow.adapter.parquet.type.LogicalType;
import org.apache.arrow.adapter.parquet.type.ParquetType;
import org.apache.arrow.adapter.parquet.type.SortOrder;

/**
 * The ColumnDescriptor encapsulates information necessary to interpret primitive column
 * data in the context of a particular schema.
 *
 * We have to examine the node structure of a column's path to the root in the schema tree
 * to be able to reassemble the nested structure from the repetition and definition levels.
 */
public class ColumnDescriptor {

  private final SchemaNode node;
  private final SchemaPrimitiveNode primitiveNode;
  private final short maxDefinitionLevel;
  private final short maxRepetitionLevel;

  public ColumnDescriptor(SchemaNode node, short maxDefinitionLevel,
                   short maxRepetitionLevel) {

    // CPP code has extra param SchemaDescriptor schema_descr, default = null, unused

    if (!node.isPrimitive()) {
      throw new ParquetException("Must be a primitive type");
    }

    this.node = node;
    this.primitiveNode = (SchemaPrimitiveNode) node;
    this.maxDefinitionLevel = maxDefinitionLevel;
    this.maxRepetitionLevel = maxRepetitionLevel;
  }

  public SchemaNode schemaNode() {
    return node;
  }

  public short maxDefinitionLevel() {
    return maxDefinitionLevel;
  }

  public short maxRepetitionLevel() {
    return maxRepetitionLevel;
  }

  @Override
  public boolean equals(Object o) {

    // Do not consider node, since primitiveNode == node
    // Also, constructor will fail if node == null

    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ColumnDescriptor that = (ColumnDescriptor) o;

    if (maxDefinitionLevel != that.maxDefinitionLevel) return false;
    if (maxRepetitionLevel != that.maxRepetitionLevel) return false;
    return primitiveNode.equals(that.primitiveNode);
  }

  @Override
  public int hashCode() {

    // Do not consider node, since primitiveNode == node
    // Also, constructor will fail if node == null

    int result = primitiveNode.hashCode();
    result = 31 * result + (int) maxDefinitionLevel;
    result = 31 * result + (int) maxRepetitionLevel;
    return result;
  }

  public String name() {
    return primitiveNode.name();
  }

  public ParquetType physicalType() {
    return primitiveNode.physicalType();
  }

  public ConvertedType convertedType() {
    return primitiveNode.convertedType();
  }

  public LogicalType logicalType() {
    return primitiveNode.logicalType();
  }

  public ColumnPath path() {
    return primitiveNode.path();
  }

  public ColumnOrder columnOrder() {
    return primitiveNode.columnOrder();
  }

  public SortOrder sortOrder() {
    LogicalType la = logicalType();
    ParquetType pt = physicalType();
    return la != null ? SortOrder.getSortOrder(la, pt) : SortOrder.getSortOrder(convertedType(), pt);
  }

  public int typeLength() {
    return primitiveNode.typeLength();
  }

  public int typePrecision() {
    return primitiveNode.decimalMetadata().precision();
  }

  public int typeScale() {
    return primitiveNode.decimalMetadata().scale();
  }

  @Override
  public String toString() {

    StringBuilder sb = new StringBuilder();

    // Always use unix-style line endings
    String eol = "\n";

    sb.append("column descriptor = {").append(eol)
        .append("  name: ").append(name()).append(",").append(eol)
        .append("  path: ").append(path().toDotString()).append(",").append(eol)
        .append("  physical_type: ").append(physicalType().name()).append(",").append(eol)
        .append("  converted_type: ").append(convertedType().name()).append(",").append(eol)
        .append("  logical_type: ").append(logicalType().toString()).append(",").append(eol)
        .append("  max_definition_level: ").append(maxDefinitionLevel()).append(",").append(eol)
        .append("  max_repetition_level: ").append(maxRepetitionLevel()).append(",").append(eol);

    if (physicalType() == ParquetType.FIXED_LEN_BYTE_ARRAY) {
      sb.append("  length: ").append(typeLength()).append(",").append(eol);
    }

    if (convertedType() == ConvertedType.DECIMAL) {
      sb.append("  precision: ").append(typePrecision()).append(",").append(eol)
          .append("  scale: ").append(typeScale()).append(",").append(eol);
    }

    sb.append("}");

    return sb.toString();
  }

}
