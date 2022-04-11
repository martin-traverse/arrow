package com.apache.arrow.adapter.parquet;

import org.apache.arrow.adapter.parquet.ColumnPath;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;


class ColumnPathTest {

    @Test
    void attrsTest() {

        ColumnPath path = new ColumnPath(Arrays.asList("toplevel", "leaf"));
        Assertions.assertEquals("toplevel.leaf", path.toDotString());

        ColumnPath path2 = ColumnPath.fromDotString("toplevel.leaf");
        Assertions.assertEquals("toplevel.leaf", path2.toDotString());

        ColumnPath extended = path2.extend("another_level");
        Assertions.assertEquals("toplevel.leaf.another_level", extended.toDotString());
    }
}
