package org.apache.cassandra.diff;

import org.junit.Assert;
import org.junit.Test;

public class YamlJobConfigurationTest {
    @Test
    public void testLoadYaml() {
        JobConfiguration jobConfiguration = load("testconfig.yaml");
        Assert.assertEquals(3, jobConfiguration.keyspaceTables().size());
        jobConfiguration.keyspaceTables().forEach(kt -> {
            Assert.assertTrue("Keyspace segment is not loaded correctly", kt.keyspace.contains("ks"));
            Assert.assertTrue("Table segment is not loaded correctly", kt.table.contains("tb"));
        });
        JobConfiguration.RetryOptions retryOptions = jobConfiguration.retryOptions();
        Assert.assertNotNull("retry_options not defined", retryOptions);
        Assert.assertNotNull("impl not defined", retryOptions.get(RetryStrategyFactory.IMPLEMENTATION_KEY));
        Assert.assertNotNull("base_delay_ms not defined", retryOptions.get(ExponentialRetryStrategy.BASE_DELAY_MS_KEY));
        Assert.assertNotNull("total_delay_ms not defined", retryOptions.get(ExponentialRetryStrategy.TOTAL_DELAY_MS_KEY));
    }

    @Test
    public void testLoadYamlWithKeyspaceTablesAbsent() {
        JobConfiguration jobConfiguration = load("test_load_config_no_keyspace_tables.yaml");
        Assert.assertNull(jobConfiguration.keyspaceTables());
        Assert.assertNull(jobConfiguration.disallowedKeyspaces());
        Assert.assertNull(jobConfiguration.filteredKeyspaceTables());
        Assert.assertTrue(jobConfiguration.shouldAutoDiscoverTables());
    }

    @Test
    public void testLoadYamlFilterOutDisallowedKeyspaces() {
        JobConfiguration jobConfiguration = load("test_load_config_all_keyspaces_filtered_out.yaml");
        Assert.assertNotNull(jobConfiguration.filteredKeyspaceTables());
        Assert.assertTrue("All tables should be filtered out", jobConfiguration.filteredKeyspaceTables().isEmpty());
        Assert.assertFalse("It should not be in the discover mode", jobConfiguration.shouldAutoDiscoverTables());
    }

    private JobConfiguration load(String filename) {
        return YamlJobConfiguration.load(getClass().getClassLoader().getResourceAsStream(filename));
    }
}
