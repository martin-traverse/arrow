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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.arrow.adapter.parquet.type.ParquetType;
import org.apache.arrow.adapter.parquet.type.SortOrder;
import org.junit.jupiter.api.Test;

public class ApplicationVersionTest {


  @Test
  void basics() {
    
    ApplicationVersion version = ApplicationVersion.fromVersionString("parquet-mr version 1.7.9");
    ApplicationVersion version1 = ApplicationVersion.fromVersionString("parquet-mr version 1.8.0");
    ApplicationVersion version2 = ApplicationVersion.fromVersionString("parquet-cpp version 1.0.0");
    ApplicationVersion version3 = ApplicationVersion.fromVersionString("");
    ApplicationVersion version4 = ApplicationVersion.fromVersionString(
        "parquet-mr version 1.5.0ab-cdh5.5.0+cd (build abcd)");
    ApplicationVersion version5 = ApplicationVersion.fromVersionString("parquet-mr");

    assertEquals("parquet-mr", version.application());
    assertEquals(1, version.version().major);
    assertEquals(7, version.version().minor);
    assertEquals(9, version.version().patch);

    assertEquals("parquet-cpp", version2.application());
    assertEquals(1, version2.version().major);
    assertEquals(0, version2.version().minor);
    assertEquals(0, version2.version().patch);

    assertEquals("parquet-mr", version4.application());
    assertEquals("abcd", version4.build());
    assertEquals(1, version4.version().major);
    assertEquals(5, version4.version().minor);
    assertEquals(0, version4.version().patch);
    assertEquals("ab", version4.version().unknown);
    assertEquals("cdh5.5.0", version4.version().preRelease);
    assertEquals("cd", version4.version().buildInfo);

    assertEquals("parquet-mr", version5.application());
    assertEquals(0, version5.version().major);
    assertEquals(0, version5.version().minor);
    assertEquals(0, version5.version().patch);

    assertTrue(version.versionLt(version1));

    EncodedStatistics stats = new EncodedStatistics();
    assertFalse(version1.hasCorrectStatistics(ParquetType.INT96, stats, SortOrder.UNKNOWN));
    assertTrue(version.hasCorrectStatistics(ParquetType.INT32, stats, SortOrder.SIGNED));
    assertFalse(version.hasCorrectStatistics(ParquetType.BYTE_ARRAY, stats, SortOrder.SIGNED));
    assertTrue(version1.hasCorrectStatistics(ParquetType.BYTE_ARRAY, stats, SortOrder.SIGNED));
    assertFalse(
        version1.hasCorrectStatistics(ParquetType.BYTE_ARRAY, stats, SortOrder.UNSIGNED));
    assertTrue(version3.hasCorrectStatistics(ParquetType.FIXED_LEN_BYTE_ARRAY, stats,
        SortOrder.SIGNED));

    // Check that the old stats are correct if min and max are the same
    // regardless of sort order
    EncodedStatistics statsStr = new EncodedStatistics();
    statsStr.setMin("a").setMax("b");
    assertFalse(
        version1.hasCorrectStatistics(ParquetType.BYTE_ARRAY, statsStr, SortOrder.UNSIGNED));
    statsStr.setMax("a");
    assertTrue(
        version1.hasCorrectStatistics(ParquetType.BYTE_ARRAY, statsStr, SortOrder.UNSIGNED));

    // Check that the same holds true for ints
    int intMin = 100;
    int intMax = 200;
    EncodedStatistics statsInt = new EncodedStatistics();

    // In CPP, converting int to str is a pointer reinterpretation: reinterpret_cast<const char*>(&intMin)
    // This will produce a four-byte string, using bytes from intMin in native byte order of the platform
    // The code below will provide four-char strings, i.e. 8 bytes in Java

    // TODO: Are statistics min / max really strings, or encoded bytes?
    // These are *encoded* statistics after all
    // IF they are bytes, perhaps EncodedStatistics should use byte array instead of string?

    statsInt
        .setMin(new String(ByteBuffer.allocate(4).putInt(intMin).array(), StandardCharsets.UTF_8))
        .setMax(new String(ByteBuffer.allocate(4).putInt(intMax).array(), StandardCharsets.UTF_8));

    assertFalse(version1.hasCorrectStatistics(ParquetType.BYTE_ARRAY, statsInt, SortOrder.UNSIGNED));

    statsInt.setMax(new String(ByteBuffer.allocate(4).putInt(intMin).array(), Charset.defaultCharset()));

    assertTrue(version1.hasCorrectStatistics(ParquetType.BYTE_ARRAY, statsInt, SortOrder.UNSIGNED));
  }

  @Test
  void empty() {
    
    ApplicationVersion version = ApplicationVersion.fromVersionString("");

    assertEquals("", version.application());
    assertEquals("", version.build());
    assertEquals(0, version.version().major);
    assertEquals(0, version.version().minor);
    assertEquals(0, version.version().patch);
    assertEquals("", version.version().unknown);
    assertEquals("", version.version().preRelease);
    assertEquals("", version.version().buildInfo);
  }

  @Test 
  void noVersion() {
    
    ApplicationVersion version = ApplicationVersion.fromVersionString("parquet-mr (build abcd)");

    assertEquals("parquet-mr (build abcd)", version.application());
    assertEquals("", version.build());
    assertEquals(0, version.version().major);
    assertEquals(0, version.version().minor);
    assertEquals(0, version.version().patch);
    assertEquals("", version.version().unknown);
    assertEquals("", version.version().preRelease);
    assertEquals("", version.version().buildInfo);
  }

  @Test 
  void versionEmpty() {
    
    ApplicationVersion version = ApplicationVersion.fromVersionString("parquet-mr version ");

    assertEquals("parquet-mr", version.application());
    assertEquals("", version.build());
    assertEquals(0, version.version().major);
    assertEquals(0, version.version().minor);
    assertEquals(0, version.version().patch);
    assertEquals("", version.version().unknown);
    assertEquals("", version.version().preRelease);
    assertEquals("", version.version().buildInfo);
  }

  @Test 
  void versionNoMajor() {
    
    ApplicationVersion version = ApplicationVersion.fromVersionString("parquet-mr version .");

    assertEquals("parquet-mr", version.application());
    assertEquals("", version.build());
    assertEquals(0, version.version().major);
    assertEquals(0, version.version().minor);
    assertEquals(0, version.version().patch);
    assertEquals("", version.version().unknown);
    assertEquals("", version.version().preRelease);
    assertEquals("", version.version().buildInfo);
  }

  @Test 
  void versionInvalidMajor() {
    
    ApplicationVersion version = ApplicationVersion.fromVersionString("parquet-mr version x1");

    assertEquals("parquet-mr", version.application());
    assertEquals("", version.build());
    assertEquals(0, version.version().major);
    assertEquals(0, version.version().minor);
    assertEquals(0, version.version().patch);
    assertEquals("", version.version().unknown);
    assertEquals("", version.version().preRelease);
    assertEquals("", version.version().buildInfo);
  }

  @Test 
  void versionMajorOnly() {
    
    ApplicationVersion version = ApplicationVersion.fromVersionString("parquet-mr version 1");

    assertEquals("parquet-mr", version.application());
    assertEquals("", version.build());
    assertEquals(1, version.version().major);
    assertEquals(0, version.version().minor);
    assertEquals(0, version.version().patch);
    assertEquals("", version.version().unknown);
    assertEquals("", version.version().preRelease);
    assertEquals("", version.version().buildInfo);
  }

  @Test 
  void versionNoMinor() {
    
    ApplicationVersion version = ApplicationVersion.fromVersionString("parquet-mr version 1.");

    assertEquals("parquet-mr", version.application());
    assertEquals("", version.build());
    assertEquals(1, version.version().major);
    assertEquals(0, version.version().minor);
    assertEquals(0, version.version().patch);
    assertEquals("", version.version().unknown);
    assertEquals("", version.version().preRelease);
    assertEquals("", version.version().buildInfo);
  }

  @Test 
  void versionMajorMinorOnly() {
    
    ApplicationVersion version = ApplicationVersion.fromVersionString("parquet-mr version 1.7");

    assertEquals("parquet-mr", version.application());
    assertEquals("", version.build());
    assertEquals(1, version.version().major);
    assertEquals(7, version.version().minor);
    assertEquals(0, version.version().patch);
    assertEquals("", version.version().unknown);
    assertEquals("", version.version().preRelease);
    assertEquals("", version.version().buildInfo);
  }

  @Test 
  void versionInvalidMinor() {
    
    ApplicationVersion version = ApplicationVersion.fromVersionString("parquet-mr version 1.x7");

    assertEquals("parquet-mr", version.application());
    assertEquals("", version.build());
    assertEquals(1, version.version().major);
    assertEquals(0, version.version().minor);
    assertEquals(0, version.version().patch);
    assertEquals("", version.version().unknown);
    assertEquals("", version.version().preRelease);
    assertEquals("", version.version().buildInfo);
  }

  @Test 
  void versionNoPatch() {
    
    ApplicationVersion version = ApplicationVersion.fromVersionString("parquet-mr version 1.7.");

    assertEquals("parquet-mr", version.application());
    assertEquals("", version.build());
    assertEquals(1, version.version().major);
    assertEquals(7, version.version().minor);
    assertEquals(0, version.version().patch);
    assertEquals("", version.version().unknown);
    assertEquals("", version.version().preRelease);
    assertEquals("", version.version().buildInfo);
  }

  @Test 
  void versionInvalidPatch() {
    
    ApplicationVersion version = ApplicationVersion.fromVersionString("parquet-mr version 1.7.x9");

    assertEquals("parquet-mr", version.application());
    assertEquals("", version.build());
    assertEquals(1, version.version().major);
    assertEquals(7, version.version().minor);
    assertEquals(0, version.version().patch);
    assertEquals("", version.version().unknown);
    assertEquals("", version.version().preRelease);
    assertEquals("", version.version().buildInfo);
  }

  @Test 
  void versionNoUnknown() {
    
    ApplicationVersion version = ApplicationVersion.fromVersionString("parquet-mr version 1.7.9-cdh5.5.0+cd");

    assertEquals("parquet-mr", version.application());
    assertEquals("", version.build());
    assertEquals(1, version.version().major);
    assertEquals(7, version.version().minor);
    assertEquals(9, version.version().patch);
    assertEquals("", version.version().unknown);
    assertEquals("cdh5.5.0", version.version().preRelease);
    assertEquals("cd", version.version().buildInfo);
  }

  @Test 
  void versionNoPreRelease() {
    
    ApplicationVersion version = ApplicationVersion.fromVersionString("parquet-mr version 1.7.9ab+cd");

    assertEquals("parquet-mr", version.application());
    assertEquals("", version.build());
    assertEquals(1, version.version().major);
    assertEquals(7, version.version().minor);
    assertEquals(9, version.version().patch);
    assertEquals("ab", version.version().unknown);
    assertEquals("", version.version().preRelease);
    assertEquals("cd", version.version().buildInfo);
  }

  @Test 
  void versionNoUnknownNoPreRelease() {
    
    ApplicationVersion version = ApplicationVersion.fromVersionString("parquet-mr version 1.7.9+cd");

    assertEquals("parquet-mr", version.application());
    assertEquals("", version.build());
    assertEquals(1, version.version().major);
    assertEquals(7, version.version().minor);
    assertEquals(9, version.version().patch);
    assertEquals("", version.version().unknown);
    assertEquals("", version.version().preRelease);
    assertEquals("cd", version.version().buildInfo);
  }

  @Test 
  void versionNoUnknownBuildInfoPreRelease() {
    
    ApplicationVersion version = ApplicationVersion.fromVersionString("parquet-mr version 1.7.9+cd-cdh5.5.0");

    assertEquals("parquet-mr", version.application());
    assertEquals("", version.build());
    assertEquals(1, version.version().major);
    assertEquals(7, version.version().minor);
    assertEquals(9, version.version().patch);
    assertEquals("", version.version().unknown);
    assertEquals("", version.version().preRelease);
    assertEquals("cd-cdh5.5.0", version.version().buildInfo);
  }

  @Test 
  void fullWithSpaces() {
    
    ApplicationVersion version = ApplicationVersion.fromVersionString(
        " parquet-mr \t version \u000b 1.5.3ab-cdh5.5.0+cd \r (build \n abcd \f) ");

    assertEquals("parquet-mr", version.application());
    assertEquals("abcd", version.build());
    assertEquals(1, version.version().major);
    assertEquals(5, version.version().minor);
    assertEquals(3, version.version().patch);
    assertEquals("ab", version.version().unknown);
    assertEquals("cdh5.5.0", version.version().preRelease);
    assertEquals("cd", version.version().buildInfo);
  }
}
