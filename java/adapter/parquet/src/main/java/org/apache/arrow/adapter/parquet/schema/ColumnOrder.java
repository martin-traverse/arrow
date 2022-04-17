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


/** Column ordering. */
public class ColumnOrder {

  /** Available column orderings. */
  public enum Type {
    UNDEFINED,
    TYPE_DEFINED_ORDER
  }

  public static final ColumnOrder UNDEFINED = new ColumnOrder(Type.UNDEFINED);
  public static final ColumnOrder TYPE_DEFINED = new ColumnOrder(Type.TYPE_DEFINED_ORDER);

  private final Type columnOrder;

  public ColumnOrder(Type columnOrder) {
    this.columnOrder = columnOrder;
  }

  // Default to Type Defined Order
  public ColumnOrder() {
    this.columnOrder = Type.TYPE_DEFINED_ORDER;
  }

  public Type getOrder() {
    return columnOrder;
  }
}
