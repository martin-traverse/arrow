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

package org.apache.arrow.adapter.parquet.properties;


/// Controls serialization format of data pages.  parquet-format v2.0.0
/// introduced a new data page metadata type DataPageV2 and serialized page
/// structure (for example, encoded levels are no longer compressed). Prior to
/// the completion of PARQUET-457 in 2020, this library did not implement
/// DataPageV2 correctly, so if you use the V2 data page format, you may have
/// forward compatibility issues (older versions of the library will be unable
/// to read the files). Note that some Parquet implementations do not implement
/// DataPageV2 at all.
public enum ParquetDataPageVersion {
  V1,
  V2
}
