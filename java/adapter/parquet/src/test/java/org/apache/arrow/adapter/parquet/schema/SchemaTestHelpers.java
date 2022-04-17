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

import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Type;

public class SchemaTestHelpers {

  public static SchemaElement newPrimitive(String name, FieldRepetitionType repetition, Type type, int fieldId) {

    SchemaElement result = new SchemaElement();
    result.setName(name);
    result.setRepetition_type(repetition);
    result.setType(type);

    if (fieldId >= 0) {
      result.setField_id(fieldId);
    }

    return result;
  }

  public static SchemaElement newGroup(String name, FieldRepetitionType repetition, int numChildren, int fieldId) {

    SchemaElement result = new SchemaElement();
    result.setName(name);
    result.setRepetition_type(repetition);
    result.setNum_children(numChildren);

    if (fieldId >= 0) {
      result.setField_id(fieldId);
    }

    return result;
  }
}
