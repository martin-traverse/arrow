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

package org.apache.arrow.adapter.parquet.type;


/** Logical type class for decimal type. */
public class DecimalMetadata {

  private final boolean isSet;
  private final int precision;
  private final int scale;

  /** Logical type class for decimal type. */
  public DecimalMetadata(boolean isSet, int precision, int scale) {
    this.isSet = isSet;
    this.precision = precision;
    this.scale = scale;
  }

  /** Create decimal metadata that is zeroed out (isSet will be false). */
  public static DecimalMetadata zeroUnset() {
    return new DecimalMetadata(false, 0, 0);
  }

  public boolean isSet() {
    return isSet;
  }

  public int precision() {
    return precision;
  }

  public int scale() {
    return scale;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) { return true; }
    if (o == null || getClass() != o.getClass()) { return false; }

    DecimalMetadata that = (DecimalMetadata) o;

    if (isSet != that.isSet) { return false; }
    if (precision != that.precision) { return false; }
    return scale == that.scale;
  }

  @Override
  public int hashCode() {
    int result = (isSet ? 1 : 0);
    result = 31 * result + precision;
    result = 31 * result + scale;
    return result;
  }
}
