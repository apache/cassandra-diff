package org.apache.cassandra.diff;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.hamcrest.CoreMatchers;
import org.hamcrest.CustomMatcher;

public class ClusterSourcedExceptionTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testCatchesExceptionHasExceptionSourceInfo() {
        expectedException.expect(ClusterSourcedException.class);
        expectedException.expectCause(CoreMatchers.isA(RuntimeException.class));
        expectedException.expectMessage("from SOURCE");
        expectedException.expect(new CustomMatcher<ClusterSourcedException>("matches the expected exceptionSource: SOURCE") {
            @Override
            public boolean matches(Object item) {
                if (item instanceof ClusterSourcedException) {
                    ClusterSourcedException ex = (ClusterSourcedException) item;
                    return ex.exceptionSource == DiffCluster.Type.SOURCE;
                }
                return false;
            }
        });
        ClusterSourcedException.catches(DiffCluster.Type.SOURCE, () -> {
            throw new RuntimeException();
        });
    }
}
