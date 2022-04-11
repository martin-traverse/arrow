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


import java.util.HashMap;
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

  private SchemaNode schema;
  // Root Node

  private final Object /* schema::GroupNode* */ groupNode;

  // Result of leaf node / tree analysis
  List<ColumnDescriptor> leaves;

  // std::unordered_map<const schema::PrimitiveNode*, int> node_to_leaf_index_;

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

  // Mapping between ColumnPath DotString to the leaf index
  private final Map<String, Integer> leafToIdx;


  /** Default constructor. */
  public SchemaDescriptor() {

    groupNode = new Object();
    leafToBase = new HashMap<>();
    leafToIdx = new HashMap<>();

  }

  // ~SchemaDescriptor() {}

  /** Init the schema descriptor using the given root schema node. */
  public void init(SchemaNode schema) {

    this.schema = schema;

    if (!schema.isGroup()) {
      throw new ParquetException("Must initialize with a schema group");
    }

    // todo

    //
    //        groupNode = (GroupNode) schema.get();
    //        leaves.clear();
    //
    //        for (int i = 0; i < groupNode.fieldCount(); ++i) {
    //            buildTree(groupNode.field(i), 0, 0, groupNode.field(i));
    //        }
  }

  /** Get column descriptor for column index i. */
  public ColumnDescriptor column(int i) {

    return null; // todo
  }

  /**
   * Get the index of a column by its dot-string path, or negative value if not found.
   *
   * If several columns share the same dot-string path, it is unspecified which one is returned.
   */
  public int columnIndex(String nodePath) {

    return -1; // todo
  }

  /** Get the index of a column by its node, or negative value if not found. */
  public int columnIndex(SchemaNode node) {

    return -1; // todo
  }

  /** Equality comparison for schema descriptors. **/
  public boolean equalTo(SchemaDescriptor other) {

    return false; // todo
  }

  /** The number of physical columns appearing in the file. **/
  public int numColumns() {
    return leaves.size();
  }

  /** Get the schema root node. */
  public SchemaNode schemaRoot() {
    return schema;
  }

  /// TODO.
  public Object /*const schema::GroupNode* */ groupNode() {
    //    return group_node_;
    return null; // todo
  }

  /** Returns the root (child of the schema root) node of the leaf(column) node. */
  public SchemaNode getColumnRoot(int i) {
    return null; // todo
  }

  public String name() {
    // return group_node_->name();
    return null; // todo
  }


  // todo

  //    public String toString() {
  //
  //    }

  //    public void updateColumnOrders(const std::vector<ColumnOrder>& column_orders) {
  //
  //    }
  //
  //    /// \brief Return column index corresponding to a particular
  //    /// PrimitiveNode. Returns -1 if not found
  //    public int getColumnIndex(const schema::PrimitiveNode& node) {
  //
  //    }

  /** Return true if any field or their children have REPEATED repetition type. **/
  public boolean hasRepeatedFields() {

    return false; // todo
  }


  private void buildTree(SchemaNode node, short maxDefLevel, short maxRepLevel, SchemaNode base) {

    // todo
  }
}
