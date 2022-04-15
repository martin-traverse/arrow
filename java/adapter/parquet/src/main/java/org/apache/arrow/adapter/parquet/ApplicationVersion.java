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

import org.apache.arrow.adapter.parquet.type.ParquetType;
import org.apache.arrow.adapter.parquet.type.SortOrder;

import java.util.Objects;


public class ApplicationVersion {

  // Application that wrote the file. e.g. "IMPALA"
  private final String application;
  // Build name
  private final String build;

  // Version of the application that wrote the file, expressed as
  // (<major>.<minor>.<patch>). Unmatched parts default to 0.
  // "1.2.3"    => {1, 2, 3}
  // "1.2"      => {1, 2, 0}
  // "1.2-cdh5" => {1, 2, 0}

  public static class Version {

    public Version(int major, int minor, int patch, String unknown, String pre_release, String build_info) {
      this.major = major;
      this.minor = minor;
      this.patch = patch;
      this.unknown = unknown;
      this.pre_release = pre_release;
      this.build_info = build_info;
    }

    final int major;
    final int minor;
    final int patch;
    final String unknown;
    final String pre_release;
    final String build_info;
  }

  private final Version version;
  // Known Versions with Issues

  public static ApplicationVersion PARQUET_251_FIXED_VERSION() {
    return new ApplicationVersion("parquet-mr", 1, 8, 0);
  }

  public static ApplicationVersion PARQUET_816_FIXED_VERSION() {
    return new ApplicationVersion("parquet-mr", 1, 2, 9);
  }

  public static ApplicationVersion PARQUET_CPP_FIXED_STATS_VERSION() {
    return new ApplicationVersion("parquet-cpp", 1, 3, 0);
  }

  public static ApplicationVersion PARQUET_MR_FIXED_STATS_VERSION() {
    return new ApplicationVersion("parquet-mr", 1, 10, 0);
  }

  public ApplicationVersion(String createdBy) {

    // TODO: Implement application version parser
    throw new ParquetException("application version string parsing not implemented yet");
  }

  public ApplicationVersion(String application, int major, int minor, int patch) {

    this.application = application;
    this.build = "";

    this.version = new Version(major, minor, patch, "", "", "");
  }

  // Returns true if version is strictly less than other_version
  public boolean versionLt(ApplicationVersion otherVersion) {

    if (!Objects.equals(application, otherVersion.application)) { return false; }

    if (version.major < otherVersion.version.major) { return true; }
    if (version.major > otherVersion.version.major) { return false; }
    if (version.minor < otherVersion.version.minor) { return true; }
    if (version.minor > otherVersion.version.minor) { return false; }
    return version.patch < otherVersion.version.patch;
  }

  // Returns true if version is strictly equal with other_version
  public boolean versionEq(ApplicationVersion otherVersion) {

    return Objects.equals(application, otherVersion.application) &&
        version.major == otherVersion.version.major &&
        version.minor == otherVersion.version.minor &&
        version.patch == otherVersion.version.patch;
  }

  // Checks if the Version has the correct statistics for a given column
  public boolean hasCorrectStatistics(ParquetType columnType, EncodedStatistics statistics, SortOrder sortOrder) {

    // default sort order = SortOrder.SIGNED

    // Reference:
    // parquet-mr/parquet-column/src/main/java/org/apache/parquet/CorruptStatistics.java
    // PARQUET-686 has more discussion on statistics

    // parquet-cpp version 1.3.0 and parquet-mr 1.10.0 onwards stats are computed
    // correctly for all types
    if (("parquet-cpp".equals(application) && versionLt(PARQUET_CPP_FIXED_STATS_VERSION())) ||
        ("parquet-mr".equals(application) && versionLt(PARQUET_MR_FIXED_STATS_VERSION()))) {

      // Only SIGNED are valid unless max and min are the same
      // (in which case the sort order does not matter)
      boolean maxEqualsMin = statistics.hasMin() && statistics.hasMax() && statistics.min().equals(statistics.max());

      if (SortOrder.SIGNED != sortOrder && !maxEqualsMin) {
        return false;
      }

      // Statistics of other types are OK
      if (columnType != ParquetType.FIXED_LEN_BYTE_ARRAY && columnType != ParquetType.BYTE_ARRAY) {
        return true;
      }
    }

    // created_by is not populated, which could have been caused by
    // parquet-mr during the same time as PARQUET-251, see PARQUET-297
    if ("unknown".equals(application)) {
      return true;
    }

    // Unknown sort order has incorrect stats
    if (SortOrder.UNKNOWN == sortOrder) {
      return false;
    }

    // PARQUET-251
    return !versionLt(PARQUET_251_FIXED_VERSION());
  }
}
