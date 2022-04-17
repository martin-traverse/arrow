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


/** Parser for Parquet version strings, to build the ApplicationVersion class. */
public class ApplicationVersionParser {

  private static final char[] spaces_ = " \t\r\n\f\u000B".toCharArray(); // \u000B = CPP \v, vertical feed
  private static final char[] digits_ = "0123456789".toCharArray();

  private final String createdBy;

  private String application;
  private String build;
  private int major;
  private int minor;
  private int patch;
  private String unknown;
  private String preRelease;
  private String buildInfo;

  // For parsing.
  private int versionParsingPosition;
  private int versionStart;
  private int versionEnd;
  private String versionString;

  /** Construct a parser for the given version string. */
  public ApplicationVersionParser(String createdBy) {
    this.createdBy = createdBy;
  }

  /** Parse the version string and return an ApplicationVersion. */
  public ApplicationVersion parse() {

    application = "unknown";
    build = "";
    major = minor = patch = 0;
    unknown = preRelease = buildInfo = "";

    boolean moreToParse = parseApplicationName();

    if (moreToParse) {
      moreToParse = parseVersion();
    }

    if (moreToParse) {
      parseBuildName();
    }

    ApplicationVersion.Version version = new ApplicationVersion.Version(
        major, minor, patch,
        unknown, preRelease, buildInfo);

    return new ApplicationVersion(application, build, version);
  }

  private boolean isSpace(String string, int offset) {

    return findFirstOf(spaces_, string.substring(offset, offset + 1), 0) >= 0;
  }

  private int removePrecedingSpaces(String string, int start, int end) {

    while (start < end && isSpace(string, start)) {
      ++start;
    }

    return start;
  }

  private int removeTrailingSpaces(String string, int start, int end) {

    while (start < (end - 1) && (end - 1) < string.length() && isSpace(string, end - 1)) {
      --end;
    }

    return end;
  }

  private boolean parseApplicationName() {

    String versionMark = " version ";
    int versionMarkPosition = createdBy.indexOf(versionMark);

    int applicationNameEnd;

    // No VERSION and BUILD_NAME.
    if (versionMarkPosition < 0) {
      versionStart = -1;
      applicationNameEnd = createdBy.length();
    } else {
      versionStart = versionMarkPosition + versionMark.length();
      applicationNameEnd = versionMarkPosition;
    }

    int applicationNameStart = 0;
    applicationNameStart = removePrecedingSpaces(createdBy, applicationNameStart, applicationNameEnd);
    applicationNameEnd = removeTrailingSpaces(createdBy, applicationNameStart, applicationNameEnd);

    application = createdBy.substring(applicationNameStart, applicationNameEnd);

    return true;
  }

  private boolean parseVersion() {

    // No VERSION.
    if (versionStart < 0) {
      return false;
    }

    versionStart = removePrecedingSpaces(createdBy, versionStart, createdBy.length());
    versionEnd = createdBy.indexOf(" (", versionStart);

    // No BUILD_NAME.
    if (versionEnd < 0) {
      versionEnd = createdBy.length();
    }

    versionEnd = removeTrailingSpaces(createdBy, versionStart, versionEnd);

    // No VERSION.
    if (versionStart == versionEnd) {
      return false;
    }

    versionString = createdBy.substring(versionStart, versionEnd);

    if (!parseVersionMajor()) {
      return false;
    }
    if (!parseVersionMinor()) {
      return false;
    }
    if (!parseVersionPatch()) {
      return false;
    }
    if (!parseVersionUnknown()) {
      return false;
    }
    if (!parseVersionPreRelease()) {
      return false;
    }
    return parseVersionBuildInfo();
  }

  private boolean parseVersionMajor() {

    int versionMajorStart = 0;
    int versionMajorEnd = findFirstNotOf(digits_, versionString, 0);

    // MAJOR only.
    if (versionMajorEnd < 0) {
      versionMajorEnd = versionString.length();
      versionParsingPosition = versionMajorEnd;
    } else {
      // No ".".
      if (versionString.charAt(versionMajorEnd) != '.') {
        return false;
      }
      // No MAJOR.
      if (versionMajorEnd == versionMajorStart) {
        return false;
      }
      versionParsingPosition = versionMajorEnd + 1; // +1 is for '.'.
    }

    String versionMajorString = versionString.substring(
        versionMajorStart, versionMajorEnd);

    // Catching number format exception will match the CPP functionality, which uses std:atoi
    // atoi returns zero if the string is not a valid integer after stripping whitespace

    if (!versionMajorString.isEmpty()) {
      try {
        major = Integer.parseInt(versionMajorString);
      } catch (NumberFormatException e) {
        minor = 0;
      }
    }

    return true;
  }

  private boolean parseVersionMinor() {

    int versionMinorStart = versionParsingPosition;
    int versionMinorEnd = findFirstNotOf(digits_, versionString, versionMinorStart);

    // MAJOR.MINOR only.
    if (versionMinorEnd < 0) {
      versionMinorEnd = versionString.length();
      versionParsingPosition = versionMinorEnd;
    } else {
      // No ".".
      if (versionString.charAt(versionMinorEnd) != '.') {
        return false;
      }
      // No MINOR.
      if (versionMinorEnd == versionMinorStart) {
        return false;
      }
      versionParsingPosition = versionMinorEnd + 1; // +1 is for '.'.
    }

    String versionMinorString = versionString.substring(
        versionMinorStart, versionMinorEnd);

    if (!versionMinorString.isEmpty()) {
      try {
        minor = Integer.parseInt(versionMinorString);
      } catch (NumberFormatException e) {
        minor = 0;
      }
    }

    return true;
  }

  private boolean parseVersionPatch() {

    int versionPatchStart = versionParsingPosition;
    int versionPatchEnd = findFirstNotOf(digits_, versionString, versionPatchStart);

    // No UNKNOWN, PRE_RELEASE and BUILD_INFO.
    if (versionPatchEnd < 0) {
      versionPatchEnd = versionString.length();
    }

    // No PATCH.
    if (versionPatchEnd == versionPatchStart) {
      return false;
    }

    String versionPatchString = versionString.substring(
        versionPatchStart, versionPatchEnd);

    try {
      patch = Integer.parseInt(versionPatchString);
    } catch (NumberFormatException e) {
      minor = 0;
    }

    versionParsingPosition = versionPatchEnd;

    return true;
  }

  private boolean parseVersionUnknown() {

    // No UNKNOWN.
    if (versionParsingPosition == versionString.length()) {
      return true;
    }

    int versionUnknownStart = versionParsingPosition;
    int versionUnknownEnd = findFirstOf(new char[]{'-', '+'}, versionString, versionUnknownStart);

    // No PRE_RELEASE and BUILD_INFO
    if (versionUnknownEnd < 0) {
      versionUnknownEnd = versionString.length();
    }

    unknown = versionString.substring(
        versionUnknownStart, versionUnknownEnd);

    versionParsingPosition = versionUnknownEnd;

    return true;
  }

  private boolean parseVersionPreRelease() {

    // No PRE_RELEASE.
    if (versionParsingPosition == versionString.length() ||
        versionString.charAt(versionParsingPosition) != '-') {
      return true;
    }

    int versionPreReleaseStart = versionParsingPosition + 1; // +1 is for '-'.
    int versionPreReleaseEnd = findFirstOf('+', versionString, versionPreReleaseStart);

    // No BUILD_INFO
    if (versionPreReleaseEnd < 0) {
      versionPreReleaseEnd = versionString.length();
    }

    preRelease = versionString.substring(
        versionPreReleaseStart, versionPreReleaseEnd);

    versionParsingPosition = versionPreReleaseEnd;

    return true;
  }

  private boolean parseVersionBuildInfo() {

    // No BUILD_INFO.
    if (versionParsingPosition == versionString.length() ||
        versionString.charAt(versionParsingPosition) != '+') {
      return true;
    }

    int versionBuildInfoStart = versionParsingPosition + 1; // +1 is for '+'.

    buildInfo = versionString.substring(versionBuildInfoStart);

    return true;
  }

  private void parseBuildName() {

    String buildMark = " (build ";
    int buildMarkPosition = createdBy.indexOf(buildMark, versionEnd);

    // No BUILD_NAME.
    if (buildMarkPosition < 0) {
      return; // false
    }

    int buildNameStart = buildMarkPosition + buildMark.length();
    buildNameStart = removePrecedingSpaces(createdBy, buildNameStart, createdBy.length());

    int buildNameEnd = findFirstOf(')', createdBy, buildNameStart);

    // No end ")".
    if (buildNameEnd < 0) {
      return; // false
    }

    buildNameEnd = removeTrailingSpaces(createdBy, buildNameStart, buildNameEnd);

    build = createdBy.substring(buildNameStart, buildNameEnd);

    // return true
  }

  private int findFirstOf(char searchChar, String str, int start) {

    for (int i = start, n = str.length(); i < n; ++i) {
      if (str.charAt(i) == searchChar) {
        return i;
      }
    }

    return -1;
  }

  private int findFirstOf(char[] searchChars, String str, int start) {

    for (int i = start, n = str.length(); i < n; ++i) {
      for (char searchChar : searchChars) {
        if (str.charAt(i) == searchChar) {
          return i;
        }
      }
    }

    return -1;
  }

  private int findFirstNotOf(char[] searchChars, String str, int start) {

    for (int i = start, n = str.length(); i < n; ++i) {

      boolean match = false;

      for (char searchChar : searchChars) {
        if (str.charAt(i) == searchChar) {
          match = true;
          break;
        }
      }

      if (!match) {
        return i;
      }
    }

    return -1;
  }
}
