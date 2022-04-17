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

package org.apache.arrow.adapter.parquet.metadata;

import java.util.Objects;

import org.apache.arrow.adapter.parquet.statistics.EncodedStatistics;
import org.apache.arrow.adapter.parquet.type.ParquetType;
import org.apache.arrow.adapter.parquet.type.SortOrder;


/** Describes the version of Parquet used to create a file. */
public class ApplicationVersion {

  // Known Versions with Issues
  public static ApplicationVersion PARQUET_251_FIXED_VERSION = new ApplicationVersion("parquet-mr", 1, 8, 0);
  public static ApplicationVersion PARQUET_816_FIXED_VERSION = new ApplicationVersion("parquet-mr", 1, 2, 9);
  public static ApplicationVersion PARQUET_CPP_FIXED_STATS_VERSION = new ApplicationVersion("parquet-cpp", 1, 3, 0);
  public static ApplicationVersion PARQUET_MR_FIXED_STATS_VERSION = new ApplicationVersion("parquet-mr", 1, 10, 0);


  // Application that wrote the file. e.g. "IMPALA"
  private final String application;
  // Build name
  private final String build;

  // Version of the application that wrote the file, expressed as
  // (<major>.<minor>.<patch>). Unmatched parts default to 0.
  // "1.2.3"    => {1, 2, 3}
  // "1.2"      => {1, 2, 0}
  // "1.2-cdh5" => {1, 2, 0}

  /** Describes the version number for a given Parquet application/library. */
  public static class Version {

    /** Construct a version number from all its parts. */
    public Version(int major, int minor, int patch, String unknown, String preRelease, String buildInfo) {
      this.major = major;
      this.minor = minor;
      this.patch = patch;
      this.unknown = unknown;
      this.preRelease = preRelease;
      this.buildInfo = buildInfo;
    }

    final int major;
    final int minor;
    final int patch;
    final String unknown;
    final String preRelease;
    final String buildInfo;
  }

  private final Version version;

  /** Construct an application version with all its parts. */
  public ApplicationVersion(String application, String build, Version version) {
    this.application = application;
    this.build = build;
    this.version = version;
  }

  /** Construct an application version for a given application name with major, minor and patch version. */
  public ApplicationVersion(String application, int major, int minor, int patch) {

    this(application, "", new Version(major, minor, patch, "", "", ""));
  }

  /** Construct an application version by parsing a version string. */
  public static ApplicationVersion fromVersionString(String createdBy) {

    ApplicationVersionParser parser = new ApplicationVersionParser(createdBy);
    return parser.parse();
  }

  public String application() {
    return application;
  }

  public String build() {
    return build;
  }

  public Version version() {
    return version;
  }

  /** Returns true if version is strictly less than otherVersion. */
  public boolean versionLt(ApplicationVersion otherVersion) {

    if (!Objects.equals(application, otherVersion.application)) { return false; }

    if (version.major < otherVersion.version.major) { return true; }
    if (version.major > otherVersion.version.major) { return false; }
    if (version.minor < otherVersion.version.minor) { return true; }
    if (version.minor > otherVersion.version.minor) { return false; }
    return version.patch < otherVersion.version.patch;
  }

  /** Returns true if version is strictly equal with otherVersion. */
  public boolean versionEq(ApplicationVersion otherVersion) {

    return Objects.equals(application, otherVersion.application) &&
        version.major == otherVersion.version.major &&
        version.minor == otherVersion.version.minor &&
        version.patch == otherVersion.version.patch;
  }

  /** Checks if the Version has the correct statistics for a given column. */
  public boolean hasCorrectStatistics(ParquetType columnType, EncodedStatistics statistics, SortOrder sortOrder) {

    // default sort order = SortOrder.SIGNED

    // Reference:
    // parquet-mr/parquet-column/src/main/java/org/apache/parquet/CorruptStatistics.java
    // PARQUET-686 has more discussion on statistics

    // parquet-cpp version 1.3.0 and parquet-mr 1.10.0 onwards stats are computed
    // correctly for all types
    if (("parquet-cpp".equals(application) && versionLt(PARQUET_CPP_FIXED_STATS_VERSION)) ||
        ("parquet-mr".equals(application) && versionLt(PARQUET_MR_FIXED_STATS_VERSION))) {

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
    return !versionLt(PARQUET_251_FIXED_VERSION);
  }
}
