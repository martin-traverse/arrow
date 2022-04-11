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

public class SchemaNode {

    public enum Type { PRIMITIVE, GROUP }


    protected SchemaNode.Type type;
    protected String name;
    protected RepetitionType repetition;
    ConvertedType convertedType;
    LogicalType logicalType;
    int fieldId;

    // Nodes should not be shared, they have a single parent.
    private final SchemaNode parent;

    protected SchemaNode(
            SchemaNode.Type type, String name,
            RepetitionType repetition,
            ConvertedType convertedType, /* = ConvertedType::NONE,*/
            int fieldId /* = -1*/) {

        this.type = type;
        this.name = name;
        this.repetition = repetition;
        this.convertedType = convertedType;
        this.logicalType = null;               // LogicalType.none();
        this.fieldId = fieldId;
        this.parent = null;
    }

    protected SchemaNode(
            SchemaNode.Type type, String name,
            RepetitionType repetition,
            LogicalType logicalType,
            int fieldId /* = -1 */) {

        this.type = type;
        this.name = name;
        this.repetition = repetition;
        this.convertedType = ConvertedType.NONE;
        this.logicalType = logicalType;
        this.fieldId = fieldId;
        this.parent = null;
    }




    // virtual ~Node() {}

    public boolean isPrimitive() {
        return type == Type.PRIMITIVE;
    }

    public boolean isGroup() {
        return type == Type.GROUP;
    }

    public boolean isOptional() {
        return repetition == RepetitionType.OPTIONAL;
    }

    public boolean isRepeated() {
        return repetition == RepetitionType.REPEATED;
    }

    public boolean  isRequired() {
        return repetition == RepetitionType.REQUIRED;
    }

    public boolean equals(SchemaNode other) {

        // todo
        return false;
    }

    public String name() {
        return name;
    }

    public Type nodeType() {
        return type;
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

    /// \brief The field_id value for the serialized SchemaElement. If the
    /// field_id is less than 0 (e.g. -1), it will not be set when serialized to
    /// Thrift.
    int fieldId() {
        return fieldId;
    }

    public SchemaNode parent() {
        return parent;
    }

    public ColumnPath path() {

        return null;  // todo
    }

    public void toParquet(Object element) {

        // TODO
    };







    protected boolean equalsInternal(SchemaNode other) {

        return false;  // todo
    }

    protected void setParent(SchemaNode p_parent) {

        // todo
    }
}
