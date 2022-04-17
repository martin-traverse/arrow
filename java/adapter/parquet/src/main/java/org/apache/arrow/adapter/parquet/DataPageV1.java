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

import org.apache.arrow.adapter.parquet.statistics.EncodedStatistics;
import org.apache.arrow.adapter.parquet.type.Encoding;
import org.apache.arrow.memory.ArrowBuf;


/** Page holder for Parquet V§ data page. */
public class DataPageV1 extends DataPage {

  private final Encoding definitionLevelEncoding;
  private final Encoding repetitionLevelEncoding;

  /** Page holder for Parquet V1 data page. */
  public DataPageV1(
      ArrowBuf buffer, int numValues,
      Encoding encoding,
      Encoding definitionLevelEncoding, Encoding repetitionLevelEncoding,
      long uncompressedSize, EncodedStatistics statistics) {

    super(PageType.DATA_PAGE, buffer, numValues, encoding, uncompressedSize, statistics);

    this.definitionLevelEncoding = definitionLevelEncoding;
    this.repetitionLevelEncoding = repetitionLevelEncoding;
  }

  public Encoding repetitionLevelEncoding() {
    return repetitionLevelEncoding;
  }

  public Encoding definitionLevelEncoding() {
    return definitionLevelEncoding;
  }
}
