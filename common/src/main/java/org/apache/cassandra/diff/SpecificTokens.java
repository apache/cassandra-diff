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

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Collections;
import java.util.Set;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableSet;

public class SpecificTokens implements Serializable, Predicate<BigInteger> {

    public static final SpecificTokens NONE = new SpecificTokens(Collections.emptySet(), Modifier.REJECT);

    public enum Modifier {REJECT, ACCEPT}

    public final ImmutableSet<BigInteger> tokens;
    public final Modifier modifier;

    public SpecificTokens(Set<BigInteger> tokens, Modifier modifier) {
        this.tokens = ImmutableSet.copyOf(tokens);
        this.modifier = modifier;
    }

    public boolean test(BigInteger token) {
        if (tokens.isEmpty())
            return true;

        // if this represents a list of tokens to accept, then this token is allowed
        // only if it is present in the list. If this is a list of tokens to reject,
        // then the opposite is true, only allow the token if it is not in the list.
        return (modifier == Modifier.ACCEPT) == tokens.contains(token);
    }

    public boolean isEmpty() {
        return tokens.isEmpty();
    }

    public String toString() {
        return String.format("SpecificTokens: [ %s, %s ]", modifier, tokens);
    }
}
