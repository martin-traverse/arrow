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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * Container for the converted Parquet schema with a computed information from
 * the schema analysis needed for file reading.
 * <p>
 * Column index to Node
 * Max repetition / definition levels for each primitive node
 * <p>
 * The ColumnDescriptor objects produced by this class can be used to assist in
 * the reconstruction of fully materialized data structures from the
 * repetition-definition level encoding of nested data.
 */
public class SchemaDescriptor {

  // Root Node
  private final SchemaNode schema;
  private final SchemaGroupNode groupNode;

  // Result of leaf node / tree analysis
  private final List<ColumnDescriptor> leaves;

  private final Map<SchemaPrimitiveNode, Integer> nodeToLeafIndex;

  // Mapping between ColumnPath DotString to the leaf index
  private final Map<String, List<Integer>> leafToIdx;

  // Mapping between leaf nodes and root group of leaf (first node
  // below the schema's root group)
  //
  // For example, the leaf `a.b.c.d` would have a link back to `a`
  //
  // -- a  <------
  // -- -- b     |
  // -- -- -- c  |
  // -- -- -- -- d
  private final Map<Integer, SchemaNode> leafToBase;


  /** Construct a schema descriptor using the given root schema node. */
  public SchemaDescriptor(SchemaNode schema) {

    if (!schema.isGroup()) {
      throw new ParquetException("Must initialize with a schema group");
    }

    this.schema = schema;
    this.groupNode = (SchemaGroupNode) schema;

    this.leaves = new ArrayList<>();
    this.nodeToLeafIndex = new HashMap<>();
    this.leafToBase = new HashMap<>();
    this.leafToIdx = new HashMap<>();

    for (int i = 0; i < groupNode.fieldCount(); ++i) {
      buildTree(groupNode.field(i), (short) 0, (short) 0, groupNode.field(i));
    }
  }

  private void buildTree(SchemaNode node, short maxDefLevel, short maxRepLevel, SchemaNode base) {

    if (node.isOptional()) {
      ++maxDefLevel;
    } else if (node.isRepeated()) {
      // Repeated fields add a definition level. This is used to distinguish
      // between an empty list and a list with an item in it.
      ++maxRepLevel;
      ++maxDefLevel;
    }

    // Now, walk the schema and create a ColumnDescriptor for each leaf node
    if (node.isGroup()) {

      SchemaGroupNode group = (SchemaGroupNode) node;

      for (int i = 0; i < group.fieldCount(); ++i) {
        buildTree(group.field(i), maxDefLevel, maxRepLevel, base);
      }

    } else {

      // CPP code passes this into ColumnDescriptor, so the column has a reference to the root schema
      // But the field is not recorded or used anywhere in ColumnDescriptor (at least at present)

      SchemaPrimitiveNode primitive = (SchemaPrimitiveNode) node;
      ColumnDescriptor column = new ColumnDescriptor(node, maxDefLevel, maxRepLevel);
      String dotString = node.path().toDotString();

      nodeToLeafIndex.put(primitive, leaves.size());

      // Primitive node, append to leaves
      leaves.add(column);
      leafToBase.put(leaves.size() - 1, base);

      // Multiple entries per dot string
      List<Integer> indices = leafToIdx.computeIfAbsent(dotString, k -> new ArrayList<>());
      indices.add(leaves.size() - 1);
    }
  }

  /**
   * Set the column order for all primitive columns.
   *
   * The number of column orders supplied must match the number of primitive columns.
   */
  // This is changed from the CPP implementation, which uses visitors to hold the iterator position
  public void updateColumnOrders(List<ColumnOrder> columnOrders) {

    Iterator<ColumnOrder> columnOrderItr = columnOrders.iterator();

    updateColumnOrders(groupNode, columnOrders.iterator());

    if (!columnOrderItr.hasNext()) {
      throw new ParquetException("Wrong number of column orders supplied (more than the number of columns)");
    }
  }

  private void updateColumnOrders(SchemaNode node, Iterator<ColumnOrder> columnOrderItr) {

    if (node.isGroup()) {

      SchemaGroupNode groupNode = (SchemaGroupNode) node;

      for (int fieldIndex = 0; fieldIndex < groupNode.fieldCount(); ++fieldIndex) {
        updateColumnOrders(groupNode.field(fieldIndex), columnOrderItr);
      }

    } else { // leaf node

      if (!columnOrderItr.hasNext()) {
        throw new ParquetException("Wrong number of column orders supplied (less than the number of columns)");
      }

      SchemaPrimitiveNode leafNode = (SchemaPrimitiveNode) node;
      ColumnOrder columnOrder = columnOrderItr.next();

      leafNode.setColumnOrder(columnOrder);
    }
  }

  public String name() {
    return groupNode.name();
  }

  public SchemaGroupNode groupNode() {
    return groupNode;
  }

  /** Get the schema root node. */
  public SchemaNode schemaRoot() {
    return schema;
  }

  /** Return true if any field or their children have REPEATED repetition type. **/
  public boolean hasRepeatedFields() {

    return groupNode.hasRepeatedFields();
  }

  /** The number of physical columns appearing in the file. **/
  public int numColumns() {
    return leaves.size();
  }

  /** Get column descriptor for column index i. */
  public ColumnDescriptor column(int columnIndex) {

    return leaves.get(columnIndex);
  }

  /**
   * Get the index of a column by its dot-string path, or negative value if not found.
   *
   * If several columns share the same dot-string path, it is unspecified which one is returned.
   */
  public int columnIndex(String nodePath) {

    List<Integer> search = leafToIdx.get(nodePath);

    if (search != null) {
      return search.get(0);
    } else {
      return -1;
    }
  }

  /** Get the index of a column by its node, or negative value if not found. */
  public int columnIndex(SchemaNode node) {

    List<Integer> search = leafToIdx.get(node.path().toDotString());

    if (search != null) {
      for (Integer idx : search) {
        if (column(idx).schemaNode().equals(node)) { // deep comparison will match reconstructed nodes
          return idx;
        }
      }
    }

    return -1;
  }

  /**
   * Return column index corresponding to a particular PrimitiveNode.
   *
   * Returns -1 if not found
   */
  public int getColumnIndex(SchemaPrimitiveNode node) {

    Integer idx = nodeToLeafIndex.get(node);

    if (idx != null) {
      return idx;
    } else {
      return -1;
    }
  }

  /** Returns the root (child of the schema root) node of the leaf(column) node. */
  public SchemaNode getColumnRoot(int columnIndex) {

    return leafToBase.get(columnIndex);
  }

  @Override
  public boolean equals(Object o) {

    if (this == o) { return true; }
    if (!(o instanceof SchemaDescriptor)) { return false; }

    SchemaDescriptor that = (SchemaDescriptor) o;

    // This mirrors the CPP implementation, which checks only the flattened leaf nodes
    // Unit test for equality fails if we compare the root schema node, which includes the top level name

    return leaves.equals(that.leaves);
  }

  @Override
  public int hashCode() {
    return leaves.hashCode();
  }

  @Override
  public String toString() {

    StringBuilder sb = new StringBuilder();

    SchemaPrinter.printSchema(groupNode, sb);

    return sb.toString();
  }

}
