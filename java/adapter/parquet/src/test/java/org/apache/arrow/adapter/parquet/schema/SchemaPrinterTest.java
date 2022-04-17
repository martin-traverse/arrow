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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.adapter.parquet.type.ConvertedType;
import org.apache.arrow.adapter.parquet.type.DecimalLogicalType;
import org.apache.arrow.adapter.parquet.type.LogicalType;
import org.apache.arrow.adapter.parquet.type.ParquetType;
import org.apache.arrow.adapter.parquet.type.RepetitionType;
import org.junit.jupiter.api.Test;


public class SchemaPrinterTest {
  
  @Test
  void examples() {

    // Test schema 1
    List<SchemaNode> fields = new ArrayList<>();
    fields.add(new SchemaPrimitiveNode("a", RepetitionType.REQUIRED, ParquetType.INT32, 1));

    // 3-level list encoding
    SchemaNode item1 = new SchemaPrimitiveNode("item1", RepetitionType.OPTIONAL, ParquetType.INT64, 4);
    SchemaNode item2 = new SchemaPrimitiveNode("item2", RepetitionType.REQUIRED, ParquetType.BOOLEAN, 5);
    SchemaNode list = new SchemaGroupNode("b", RepetitionType.REPEATED,
        Arrays.asList(item1, item2), ConvertedType.LIST, 3);
    SchemaNode bag = new SchemaGroupNode("bag", RepetitionType.OPTIONAL,
        Collections.singletonList(list), (LogicalType) null, 2);
    fields.add(bag);

    fields.add(new SchemaPrimitiveNode("c", RepetitionType.REQUIRED, ParquetType.INT32,
        ConvertedType.DECIMAL, -1, 3, 2, 6));

    fields.add(new SchemaPrimitiveNode("d", RepetitionType.REQUIRED,
        new DecimalLogicalType(10, 5), ParquetType.INT64, /* length = */ -1, 7));

    SchemaNode schema = new SchemaGroupNode("schema", RepetitionType.REPEATED, fields, (LogicalType) null, 0);

    String result = SchemaPrinter.printSchema(schema);

    String eol = "\n";
    String expected = "repeated group field_id=0 schema {" + eol +
        "  required int32 field_id=1 a;" + eol +
        "  optional group field_id=2 bag {" + eol +
        "    repeated group field_id=3 b (List) {" + eol +
        "      optional int64 field_id=4 item1;" + eol +
        "      required boolean field_id=5 item2;" + eol +
        "    }" + eol +
        "  }" + eol +
        "  required int32 field_id=6 c (Decimal(precision=3, scale=2));" + eol +
        "  required int64 field_id=7 d (Decimal(precision=10, scale=5));" + eol +
        "}" + eol;

    assertEquals(expected, result);
  }
}
