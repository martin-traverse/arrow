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


/** Base type for DataPageV1 and DataPageV2 including common attributes. */
public abstract class DataPage extends Page {

  protected final int numValues;
  protected final Encoding encoding;
  protected final long uncompressedSize;
  protected final EncodedStatistics statistics;

  /** Base type for DataPageV1 and DataPageV2 including common attributes. */
  protected DataPage(
      PageType type, ArrowBuf buffer,
      int numValues, Encoding encoding, long uncompressedSize, EncodedStatistics statistics) {

    super(buffer, type);

    this.numValues = numValues;
    this.encoding = encoding;
    this.uncompressedSize = uncompressedSize;
    this.statistics = statistics;
  }

  public int numValues() {
    return numValues;
  }

  public Encoding encoding() {
    return encoding;
  }

  public long uncompressedSize() {
    return uncompressedSize;
  }

  public EncodedStatistics statistics() {
    return statistics;
  }
}
