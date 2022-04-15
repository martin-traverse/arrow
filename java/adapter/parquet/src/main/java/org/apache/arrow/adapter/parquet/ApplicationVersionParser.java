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

public class ApplicationVersionParser {

  private static final String spaces_ = " \t\r\n\f"; // \v  todo
  private static final String digits_ = "0123456789";

  private String created_by_;
  private ApplicationVersion application_version_;

  private String application;
  private ApplicationVersion.Version version;

  // For parsing.
  private long version_parsing_position_;
  private long version_start_;
  private long version_end_;
  private String version_string_;

  public ApplicationVersionParser(String created_by, ApplicationVersion application_version) {
    this.created_by_ = created_by;
    this.application_version_ = application_version;
  }

  public void Parse() {

    application = "unknown";
    version = new ApplicationVersion.Version(0, 0, 0, "", "", "");

    if (!parseApplicationName()) {
      return;
    }
    if (!ParseVersion()) {
      return;
    }
    if (!ParseBuildName()) {
      return;
    }
  }

  private boolean isSpace(String string, int offset /*ref */) {

    // auto target = ::arrow::util::string_view(string).substr(offset, 1);
    // return target.find_first_of(spaces_) != ::arrow::util::string_view::npos;

    return spaces_.contains(string.substring(offset, 1));
  }

  private int removePrecedingSpaces(String string, int start, int end) {

    // CPP has start, end as references
    // while (start < end && IsSpace(string, start)) {
    //   ++start;
    // }

    while (start < end && isSpace(string, start)) {
      ++start;
    }

    return start;
  }

  private int removeTrailingSpaces(String string, int start, int end) {

    // CPP has start, end as references
    // while (start < (end - 1) && (end - 1) < string.size() && IsSpace(string, end - 1)) {
    //   --end;
    // }

    while (start < (end - 1) && (end -1) < string.length() && isSpace(string, end - 1)) {
      --end;
    }

    return end;
  }

  private boolean parseApplicationName() {

    String version_mark = " version ";
    int version_mark_position = created_by_.indexOf(version_mark);

    int application_name_end;

    // No VERSION and BUILD_NAME.
    if (version_mark_position < 0) {
      version_start_ = -1;
      application_name_end = created_by_.length();
    } else {
      version_start_ = version_mark_position + version_mark.length();
      application_name_end = version_mark_position;
    }

    int application_name_start = 0;
    application_name_start = removePrecedingSpaces(created_by_, application_name_start, application_name_end);
    application_name_end = removeTrailingSpaces(created_by_, application_name_start, application_name_end);

    application = created_by_.substring(application_name_start, application_name_end - application_name_start);

    return true;
  }

    bool ParseVersion() {
      // No VERSION.
      if (version_start_ == std::string::npos) {
        return false;
      }

      RemovePrecedingSpaces(created_by_, version_start_, created_by_.size());
      version_end_ = created_by_.find(" (", version_start_);
      // No BUILD_NAME.
      if (version_end_ == std::string::npos) {
        version_end_ = created_by_.size();
      }
      RemoveTrailingSpaces(created_by_, version_start_, version_end_);
      // No VERSION.
      if (version_start_ == version_end_) {
        return false;
      }
      version_string_ = created_by_.substr(version_start_, version_end_ - version_start_);

      if (!ParseVersionMajor()) {
        return false;
      }
      if (!ParseVersionMinor()) {
        return false;
      }
      if (!ParseVersionPatch()) {
        return false;
      }
      if (!ParseVersionUnknown()) {
        return false;
      }
      if (!ParseVersionPreRelease()) {
        return false;
      }
      if (!ParseVersionBuildInfo()) {
        return false;
      }

      return true;
    }

    bool ParseVersionMajor() {
      size_t version_major_start = 0;
      auto version_major_end = version_string_.find_first_not_of(digits_);
      // MAJOR only.
      if (version_major_end == std::string::npos) {
        version_major_end = version_string_.size();
        version_parsing_position_ = version_major_end;
      } else {
        // No ".".
        if (version_string_[version_major_end] != '.') {
          return false;
        }
        // No MAJOR.
        if (version_major_end == version_major_start) {
          return false;
        }
        version_parsing_position_ = version_major_end + 1;  // +1 is for '.'.
      }
      auto version_major_string = version_string_.substr(
          version_major_start, version_major_end - version_major_start);
      application_version_.version.major = atoi(version_major_string.c_str());
      return true;
    }

    bool ParseVersionMinor() {
      auto version_minor_start = version_parsing_position_;
      auto version_minor_end =
          version_string_.find_first_not_of(digits_, version_minor_start);
      // MAJOR.MINOR only.
      if (version_minor_end == std::string::npos) {
        version_minor_end = version_string_.size();
        version_parsing_position_ = version_minor_end;
      } else {
        // No ".".
        if (version_string_[version_minor_end] != '.') {
          return false;
        }
        // No MINOR.
        if (version_minor_end == version_minor_start) {
          return false;
        }
        version_parsing_position_ = version_minor_end + 1;  // +1 is for '.'.
      }
      auto version_minor_string = version_string_.substr(
          version_minor_start, version_minor_end - version_minor_start);
      application_version_.version.minor = atoi(version_minor_string.c_str());
      return true;
    }

    bool ParseVersionPatch() {
      auto version_patch_start = version_parsing_position_;
      auto version_patch_end =
          version_string_.find_first_not_of(digits_, version_patch_start);
      // No UNKNOWN, PRE_RELEASE and BUILD_INFO.
      if (version_patch_end == std::string::npos) {
        version_patch_end = version_string_.size();
      }
      // No PATCH.
      if (version_patch_end == version_patch_start) {
        return false;
      }
      auto version_patch_string = version_string_.substr(
          version_patch_start, version_patch_end - version_patch_start);
      application_version_.version.patch = atoi(version_patch_string.c_str());
      version_parsing_position_ = version_patch_end;
      return true;
    }

    bool ParseVersionUnknown() {
      // No UNKNOWN.
      if (version_parsing_position_ == version_string_.size()) {
        return true;
      }
      auto version_unknown_start = version_parsing_position_;
      auto version_unknown_end = version_string_.find_first_of("-+", version_unknown_start);
      // No PRE_RELEASE and BUILD_INFO
      if (version_unknown_end == std::string::npos) {
        version_unknown_end = version_string_.size();
      }
      application_version_.version.unknown = version_string_.substr(
          version_unknown_start, version_unknown_end - version_unknown_start);
      version_parsing_position_ = version_unknown_end;
      return true;
    }

    bool ParseVersionPreRelease() {
      // No PRE_RELEASE.
      if (version_parsing_position_ == version_string_.size() ||
          version_string_[version_parsing_position_] != '-') {
        return true;
      }

      auto version_pre_release_start = version_parsing_position_ + 1;  // +1 is for '-'.
      auto version_pre_release_end =
          version_string_.find_first_of("+", version_pre_release_start);
      // No BUILD_INFO
      if (version_pre_release_end == std::string::npos) {
        version_pre_release_end = version_string_.size();
      }
      application_version_.version.pre_release = version_string_.substr(
          version_pre_release_start, version_pre_release_end - version_pre_release_start);
      version_parsing_position_ = version_pre_release_end;
      return true;
    }

    bool ParseVersionBuildInfo() {
      // No BUILD_INFO.
      if (version_parsing_position_ == version_string_.size() ||
          version_string_[version_parsing_position_] != '+') {
        return true;
      }

      auto version_build_info_start = version_parsing_position_ + 1;  // +1 is for '+'.
      application_version_.version.build_info =
          version_string_.substr(version_build_info_start);
      return true;
    }

    bool ParseBuildName() {
      std::string build_mark(" (build ");
      auto build_mark_position = created_by_.find(build_mark, version_end_);
      // No BUILD_NAME.
      if (build_mark_position == std::string::npos) {
        return false;
      }
      auto build_name_start = build_mark_position + build_mark.size();
      RemovePrecedingSpaces(created_by_, build_name_start, created_by_.size());
      auto build_name_end = created_by_.find_first_of(")", build_name_start);
      // No end ")".
      if (build_name_end == std::string::npos) {
        return false;
      }
      RemoveTrailingSpaces(created_by_, build_name_start, build_name_end);
      application_version_.build_ =
          created_by_.substr(build_name_start, build_name_end - build_name_start);

      return true;
    }
}
