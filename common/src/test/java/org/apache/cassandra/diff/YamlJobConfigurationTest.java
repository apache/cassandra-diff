package org.apache.cassandra.diff;

import org.junit.Assert;
import org.junit.Test;

public class YamlJobConfigurationTest {
    @Test
    public void testLoadYaml() {
        JobConfiguration jobConfiguration = YamlJobConfiguration.load("src/test/resources/testconfig.yaml");
        Assert.assertEquals(3, jobConfiguration.keyspaceTables().size());
        jobConfiguration.keyspaceTables().forEach(kt -> {
            Assert.assertTrue("Keyspace segment is not loaded correctly", kt.keyspace.contains("ks"));
            Assert.assertTrue("Table segment is not loaded correctly", kt.table.contains("tb"));
        });
    }
}
