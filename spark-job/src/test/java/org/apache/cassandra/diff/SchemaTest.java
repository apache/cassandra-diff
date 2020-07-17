package org.apache.cassandra.diff;

import java.util.HashSet;

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

public class SchemaTest {
    private static final KeyspaceTablePair kt1 = new KeyspaceTablePair("ks", "tbl1");
    private static final KeyspaceTablePair kt2 = new KeyspaceTablePair("ks", "tbl2");
    private static final KeyspaceTablePair kt3 = new KeyspaceTablePair("ks", "tbl3");

    @Test
    public void testIntersectSameInstance() {
        Schema schemaA = new Schema(new HashSet<>());
        Schema intersection = schemaA.intersect(schemaA);
        Assert.assertSame(schemaA, intersection);
    }

    @Test
    public void testIntersectIsCommutative() {
        Schema schemaA = new Schema(ImmutableSet.of(kt1, kt2));
        Schema schemaB = new Schema(ImmutableSet.of(kt1, kt3));
        Schema intersection1 = schemaA.intersect(schemaB);
        Schema intersection2 = schemaB.intersect(schemaA);
        Assert.assertEquals(1, intersection1.size());
        Assert.assertEquals(intersection1.toQualifiedTableList(), intersection2.toQualifiedTableList());
    }

    @Test
    public void testToQualifiedTableList() {
        Schema schema = new Schema(ImmutableSet.of(kt1, kt2, kt3));
        Assert.assertEquals(3, schema.size());
        Assert.assertEquals(ImmutableSet.of(kt1, kt2, kt3), ImmutableSet.copyOf(schema.toQualifiedTableList()));
    }
}
