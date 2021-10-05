/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.diff;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DiffJobTest
{
    @Test
    public void testSplitsM3P()
    {
        splitTestHelper(TokenHelper.forPartitioner("Murmur3Partitioner"));
    }
    @Test
    public void testSplitsRandom()
    {
        splitTestHelper(TokenHelper.forPartitioner("RandomPartitioner"));
    }

    @Test
    public void testGetJobParamsWithJobIdProvidedShouldReturnNonNullConFigParams() {
        final MockConfig mockConfig = new MockConfig();
        final JobMetadataDb.JobLifeCycle mockJob = mock(JobMetadataDb.JobLifeCycle.class);
        final List<KeyspaceTablePair> keyspaceTablePairs = new ArrayList<>();
        final DiffJob.Params params = DiffJob.getJobParams(mockJob, mockConfig, keyspaceTablePairs);
        assertNotNull(params);
    }

    @Test
    public void testGetJobParamsDuringRetryShouldReturnPreviousParams() {
        final MockConfig mockConfig = new MockConfig();
        final JobMetadataDb.JobLifeCycle mockJob = mock(JobMetadataDb.JobLifeCycle.class);
        final DiffJob.Params mockParams = mock(DiffJob.Params.class);
        when(mockJob.getJobParams(any())).thenAnswer(invocationOnMock -> mockParams);
        final List<KeyspaceTablePair> keyspaceTablePairs = new ArrayList<>();
        final DiffJob.Params params = DiffJob.getJobParams(mockJob, mockConfig, keyspaceTablePairs);
        assertEquals(params, mockParams);
    }

    @Test
    public void testGetJobParamsWithNoJobId() {
        final MockConfig mockConfig = mock(MockConfig.class);
        when(mockConfig.jobId()).thenReturn(Optional.empty());

        final JobMetadataDb.JobLifeCycle mockJob = mock(JobMetadataDb.JobLifeCycle.class);
        final List<KeyspaceTablePair> keyspaceTablePairs = new ArrayList<>();
        final DiffJob.Params params = DiffJob.getJobParams(mockJob, mockConfig, keyspaceTablePairs);
        assertNotNull(params.jobId);
    }

    private void splitTestHelper(TokenHelper tokens)
    {
        List<DiffJob.Split> splits = DiffJob.calculateSplits(50, 1, tokens);
        assertEquals(splits.get(0).start, tokens.min());
        DiffJob.Split prevSplit = null;
        for (DiffJob.Split split : splits)
        {
            if (prevSplit != null)
                assertEquals(prevSplit.end, split.start.subtract(BigInteger.ONE));
            prevSplit = split;
        }
        assertEquals(splits.get(splits.size() - 1).end, tokens.max());
        for (int i = 0; i < splits.size(); i++)
            assertEquals(i, splits.get(i).splitNumber);
    }

    private class MockConfig extends AbstractMockJobConfiguration {
        @Override
        public int splits() {
            return 2;
        }

        @Override
        public int buckets() {
            return 2;
        }

        @Override
        public Optional<UUID> jobId() {
            return Optional.of(UUID.randomUUID());
        }

        @Override
        public double partitionSamplingProbability() {
            return 1;
        }
    }
}
