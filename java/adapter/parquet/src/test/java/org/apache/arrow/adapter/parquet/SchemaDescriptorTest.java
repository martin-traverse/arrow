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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.arrow.adapter.parquet.type.ParquetType;
import org.apache.arrow.adapter.parquet.type.RepetitionType;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;


public class SchemaDescriptorTest {

  SchemaDescriptor descr_;

  @Test
  void initNonGroup() {

    SchemaNode node = new SchemaPrimitiveNode("field", RepetitionType.OPTIONAL, ParquetType.INT32);

    assertThrows(ParquetException.class, () -> new SchemaDescriptor(node));
  }

  @Disabled
  @Test
  void equals() {

    fail();
  }

  @Disabled
  @Test
  void buildTree() {

    fail();
  }

  @Disabled
  @Test
  void hasRepeatedFields() {

    fail();
  }
}
