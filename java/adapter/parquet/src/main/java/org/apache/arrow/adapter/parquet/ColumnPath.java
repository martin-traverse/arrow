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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * Representation of the hierarchical path for a column.
 */
public class ColumnPath {

  private final List<String> path;

  /** Default constructor, column path is empty. **/
  public ColumnPath() {
    path = new ArrayList<>();
  }

  /** Construct a column for the given path. **/
  public ColumnPath(List<String> path) {
    this.path = path;
  }

  /** Construct a column path from a dotted string. **/
  public static ColumnPath fromDotString(String dotString) {

    String[] items = dotString.split("\\.");
    List<String> path = Arrays.asList(items);

    return new ColumnPath(path);
  }

  /** Construct a column path from a schema node, by traversing the node hierarchy. **/
  public static ColumnPath fromNode(SchemaNode node) {

    // Build the path in reverse order as we traverse the nodes to the top
    List<String> rpath = new ArrayList<>();

    SchemaNode cursor = node;

    // The schema node is not part of the ColumnPath
    while (cursor.parent() != null) {
      rpath.add(cursor.name());
      cursor = cursor.parent();
    }

    // Build ColumnPath in correct order
    Collections.reverse(rpath);
    return new ColumnPath(rpath);
  }

  /** Create a new column path, by extending this path with an extra node (the current path is not modified). **/
  public ColumnPath extend(String nodeName) {

    List<String> newPath = new ArrayList<>(path.size() + 1);
    newPath.addAll(path);
    newPath.add(nodeName);

    return new ColumnPath(newPath);
  }

  /** Build the dot string notation for this column path. **/
  public String toDotString() {

    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < path.size(); i++) {

      if (i != 0) {
        sb.append(".");
      }

      sb.append(path.get(i));
    }

    return sb.toString();
  }

  /** Get the column path as a list of string segments. **/
  public List<String> toDotVector() {
    return path;
  }
}