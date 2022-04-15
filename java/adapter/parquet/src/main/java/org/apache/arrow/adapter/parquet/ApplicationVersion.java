package org.apache.arrow.adapter.parquet;

import org.apache.arrow.adapter.parquet.type.ParquetType;
import org.apache.arrow.adapter.parquet.type.SortOrder;

public class ApplicationVersion {

  // Application that wrote the file. e.g. "IMPALA"
  String application_;
  // Build name
  String build_;

  // Version of the application that wrote the file, expressed as
  // (<major>.<minor>.<patch>). Unmatched parts default to 0.
  // "1.2.3"    => {1, 2, 3}
  // "1.2"      => {1, 2, 0}
  // "1.2-cdh5" => {1, 2, 0}

  public static class Version {
    int major;
    int minor;
    int patch;
    String unknown;
    String pre_release;
    String build_info;
  }

  Version version;

  // Known Versions with Issues
  public static ApplicationVersion PARQUET_251_FIXED_VERSION() {

  }
  public static ApplicationVersion PARQUET_816_FIXED_VERSION() {

  }
  public static ApplicationVersion PARQUET_CPP_FIXED_STATS_VERSION() {

  }
  public static ApplicationVersion PARQUET_MR_FIXED_STATS_VERSION() {

  }

  public ApplicationVersion(String created_by) {

  }
  public ApplicationVersion(String application, int major, int minor, int patch) {

  }

  // Returns true if version is strictly less than other_version
  public boolean VersionLt(ApplicationVersion other_version) {

  }

  // Returns true if version is strictly equal with other_version
  public boolean versionEq(ApplicationVersion other_version) {

  }

  // Checks if the Version has the correct statistics for a given column
  public boolean hasCorrectStatistics(ParquetType primitive, EncodedStatistics statistics, SortOrder sort_order) {

    // default sort order = SortOrder.SIGNED

  }
}
