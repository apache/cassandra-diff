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
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.diff.DiffJob;
import org.apache.cassandra.diff.TokenHelper;

import static org.junit.Assert.assertEquals;

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
}
