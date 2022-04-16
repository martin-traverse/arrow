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


/** Base class for Parquet page holders. */
public abstract class Page {

  // TODO: Parallel processing is not yet safe because of memory-ownership semantics
  // (the PageReader may or may not own the memory referenced by a page)

  // TODO(wesm): In the future Parquet implementations may store the crc code in format::PageHeader.
  // parquet-mr currently does not, so we also skip it here, both on the read and write path

  private final ArrowBuf buffer;
  private final PageType pageType;

  public Page(ArrowBuf buffer, PageType pageType) {
    this.buffer = buffer;
    this.pageType = pageType;
  }

  public PageType pageType() {
    return pageType;
  }

  public ArrowBuf buffer() {
    return buffer;
  }

  // TODO: Remove these after implementation work complete

  // @returns: a pointer to the page's data
  /** Using this method does not make sense for Java implementation. */
  @Deprecated
  public byte[] data() {
    return buffer.nioBuffer().array();
  }

  // @returns: the total size in bytes of the page's data buffer
  /** Using this method does not make sense for Java implementation. */
  @Deprecated
  public long size() {
    return buffer.readerIndex() + buffer.readableBytes();
  }
}
