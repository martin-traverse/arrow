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

import org.apache.arrow.memory.ArrowBuf;


/** Page holder for Parquet V2 data page. */
public class DataPageV2 extends DataPage {

  private final int numNulls;
  private final int numRows;
  private final int definitionLevelsByteLength;
  private final int repetitionLevelsByteLength;
  private final boolean isCompressed;

  /** Page holder for Parquet V2 data page. */
  public DataPageV2(
      ArrowBuf buffer,
      int numValues, int numNulls, int numRows,
      Encoding encoding,
      int definitionLevelsByteLength, int repetitionLevelsByteLength,
      long uncompressedSize, boolean isCompressed /* = false */,
      EncodedStatistics statistics /* = EncodedStatistics() */) {

    super(PageType.DATA_PAGE_V2, buffer, numValues, encoding, uncompressedSize, statistics);

    this.numNulls = numNulls;
    this.numRows = numRows;
    this.definitionLevelsByteLength = definitionLevelsByteLength;
    this.repetitionLevelsByteLength = repetitionLevelsByteLength;
    this.isCompressed = isCompressed;
  }

  public int numNulls() {
    return numNulls;
  }

  public int numRows() {
    return numRows;
  }

  public int definitionLevelsByteLength() {
    return definitionLevelsByteLength;
  }

  public int repetitionLevelsByteLength() {
    return repetitionLevelsByteLength;
  }

  public boolean isCompressed() {
    return isCompressed;
  }
}
