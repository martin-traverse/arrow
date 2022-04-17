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

import org.apache.arrow.adapter.parquet.type.Encoding;
import org.apache.arrow.memory.ArrowBuf;


/** Page holder for Parquet dictionary page. */
public class DictionaryPage extends Page {

  private final int numValues;
  private final Encoding encoding;
  private final boolean isSorted;

  /** Page holder for Parquet dictionary page. */
  public DictionaryPage(ArrowBuf buffer, int numValues, Encoding encoding, boolean isSorted /* = false */) {

    super(buffer, PageType.DICTIONARY_PAGE);

    this.numValues = numValues;
    this.encoding = encoding;
    this.isSorted = isSorted;
  }

  public int numValues() {
    return numValues;
  }

  public Encoding encoding() {
    return encoding;
  }

  public boolean isSorted() {
    return isSorted;
  }


}
