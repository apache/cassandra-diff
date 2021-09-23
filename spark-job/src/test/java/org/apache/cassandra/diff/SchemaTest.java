package org.apache.cassandra.diff;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

public class SchemaTest {
    private static final KeyspaceTablePair kt1 = new KeyspaceTablePair("ks", "tbl1");
    private static final KeyspaceTablePair kt2 = new KeyspaceTablePair("ks", "tbl2");
    private static final KeyspaceTablePair kt3 = new KeyspaceTablePair("ks", "tbl3");

    private class MockConfig extends AbstractMockJobConfiguration {
        private List<String> disallowedKeyspaces;

        MockConfig(List<String> disallowedKeyspaces) {
            this.disallowedKeyspaces = disallowedKeyspaces;
        }

        @Nullable
        @Override
        public List<String> disallowedKeyspaces() {
            return disallowedKeyspaces;
        }

        @Override
        public double partitionSamplingProbability() {
            return 1;
        }
    }

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

    @Test
    public void testGetKeyspaceFilterWithDefault() {
        MockConfig config = new MockConfig(null);
        Set<String> filter = Schema.getKeyspaceFilter(config);
        Assert.assertFalse("Default fileter should not be empty", filter.isEmpty());
    }

    @Test
    public void testGetKeyspaceFilterWithAdditions() {
        List<String> disallowed = Arrays.asList("ks1, ks2");
        MockConfig configWithDefault = new MockConfig(null);
        MockConfig configWithAddition = new MockConfig(disallowed);
        Set<String> defaultFilter = Schema.getKeyspaceFilter(configWithDefault);
        Set<String> filter = Schema.getKeyspaceFilter(configWithAddition);
        Assert.assertFalse("Filter should not be not empty", filter.isEmpty());
        Assert.assertEquals("Filter with additions should be larger than the default", disallowed.size(), filter.size() - defaultFilter.size());
        disallowed.forEach(ks -> Assert.assertTrue("Filter should contain the additional disallowed keyspace.", filter.contains(ks)));
    }

    @Test
    public void testSchemaDifference() {
        Schema first = new Schema(ImmutableSet.of(kt1, kt2));
        Schema second = new Schema(ImmutableSet.of(kt2, kt3));
        Pair<Set<KeyspaceTablePair>, Set<KeyspaceTablePair>> difference = Schema.difference(first, second);
        Assert.assertTrue("Should contain the distinct table in first schema", difference.getLeft().contains(kt1));
        Assert.assertTrue("Should contain the distinct table in second schema", difference.getRight().contains(kt3));
    }
}
