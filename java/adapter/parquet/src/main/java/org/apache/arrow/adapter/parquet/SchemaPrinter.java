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
import org.apache.arrow.adapter.parquet.type.RepetitionType;
import org.apache.commons.lang3.StringUtils;


/** Text string printer to pretty-print a formatted schema definition. */
public class SchemaPrinter {

  private static final String EOL = "\n"; // always use Unix-style line endings
  private static final int INDENT_WIDTH = 2;

  /** Pretty-print a text representation of the given schema node. */
  public static String printSchema(SchemaNode schema) {

    StringBuilder sb = new StringBuilder();

    printNode(schema, sb, 0);

    return sb.toString();
  }

  private static void printNode(SchemaNode node, StringBuilder sb, int indent) {

    indent(sb, indent);

    if (node.isGroup()) {
      printGroupNode((SchemaGroupNode) node, sb, indent);
    } else {
      printPrimitiveNode((SchemaPrimitiveNode) node, sb);
    }
  }

  private static void printGroupNode(SchemaGroupNode node, StringBuilder sb, int indent) {

    printRepLevel(node.repetition(), sb);
    sb.append(" group ").append("field_id=").append(node.fieldId()).append(" ").append(node.name());

    ConvertedType lt = node.convertedType();
    LogicalType la = node.logicalType();

    if (la != null && la.isValid() && !la.isNone()) {
      sb.append(" (").append(la).append(")");
    } else if (lt != ConvertedType.NONE) {
      sb.append(" (").append(lt.name()).append(")");
    }

    sb.append(" {").append(EOL);
    indent += INDENT_WIDTH;

    for (int i = 0; i < node.fieldCount(); ++i) {
      printNode(node.field(i), sb, indent);
    }

    indent -= INDENT_WIDTH;
    indent(sb, indent);

    sb.append("}").append(EOL);
  }

  private static void printPrimitiveNode(SchemaPrimitiveNode node, StringBuilder sb) {

    printRepLevel(node.repetition(), sb);
    sb.append(" ");
    printType(node, sb);
    sb.append(" field_id=").append(node.fieldId()).append(" ").append(node.name());
    printConvertedType(node, sb);
    sb.append(";").append(EOL);
  }

  private static void printConvertedType(SchemaPrimitiveNode node, StringBuilder sb) {

    ConvertedType lt = node.convertedType();
    LogicalType la = node.logicalType();

    if (la != null && la.isValid() && !la.isNone()) {

      sb.append(" (").append(la).append(")");

    } else if (lt == ConvertedType.DECIMAL) {

      sb.append(" (").append(lt.name()).append("(")
          .append(node.decimalMetadata().precision()).append(",").append(node.decimalMetadata().scale())
          .append("))");

    } else if (lt != ConvertedType.NONE) {

      sb.append(" (").append(lt.name()).append(")");
    }
  }

  private static void printType(SchemaPrimitiveNode node, StringBuilder sb) {

    switch (node.physicalType()) {
      case BOOLEAN:
        sb.append("boolean");
        break;
      case INT32:
        sb.append("int32");
        break;
      case INT64:
        sb.append("int64");
        break;
      case INT96:
        sb.append("int96");
        break;
      case FLOAT:
        sb.append("float");
        break;
      case DOUBLE:
        sb.append("double");
        break;
      case BYTE_ARRAY:
        sb.append("binary");
        break;
      case FIXED_LEN_BYTE_ARRAY:
        sb.append("fixed_len_byte_array(").append(node.typeLength()).append(")");
        break;
      default:
        break;
    }
  }

  private static void printRepLevel(RepetitionType repetition, StringBuilder sb) {

    switch (repetition) {
      case REQUIRED:
        sb.append("required");
        break;
      case OPTIONAL:
        sb.append("optional");
        break;
      case REPEATED:
        sb.append("repeated");
        break;
      default:
        break;
    }
  }

  private static void indent(StringBuilder sb, int indent) {

    // Apache commons is already a dependency, so StringUtils is available

    if (indent > 0) {
      sb.append(StringUtils.repeat(' ', indent));
    }
  }

}
