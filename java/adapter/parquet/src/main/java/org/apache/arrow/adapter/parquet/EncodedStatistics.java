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

public class EncodedStatistics {

  private String max, min;
  private boolean isSigned = false;

  private long nullCount = 0;
  private long distinctCount = 0;

  private boolean hasMin = false;
  private boolean hasMax = false;
  private boolean hasNullCount = false;
  private boolean hasDistinctCount = false;

  public EncodedStatistics() {
    max = "";
    min = "";
  }

  public String max() {
    return max;
  }

  public String min() {
    return min;
  }

  public boolean hasMin() {
    return hasMin;
  }

  public boolean hasMax() {
    return hasMax;
  }

  // From parquet-mr
  // Don't write stats larger than the max size rather than truncating. The
  // rationale is that some engines may use the minimum value in the page as
  // the true minimum for aggregations and there is no way to mark that a
  // value has been truncated and is a lower bound and not in the page.
  void applyStatSizeLimits(long length) {
    if (max.length() > length) {
      hasMax = false;
    }
    if (min.length() > length) {
      hasMin = false;
    }
  }

  public boolean isSet() {
    return hasMin || hasMax || hasNullCount || hasDistinctCount;
  }

  public boolean isSigned() {
    return isSigned;
  }

  public void setIsSigned(boolean isSigned) {
    this.isSigned = isSigned;
  }

  public EncodedStatistics setMax(String value) {
    max = value;
    hasMax = true;
    return this;
  }

  public EncodedStatistics setMin(String value) {
    min = value;
    hasMin = true;
    return this;
  }

  public EncodedStatistics setNullCount(long value) {
    nullCount = value;
    hasNullCount = true;
    return this;
  }

  public EncodedStatistics setDistinctCount(long value) {
    distinctCount = value;
    hasDistinctCount = true;
    return this;
  }
}
