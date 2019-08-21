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

import java.util.Map;

import com.datastax.driver.core.Cluster;

public class ContactPointsClusterProvider implements ClusterProvider {
    private static final String PREFIX = "diff.cluster";
    private static final String USERNAME_KEY = "cql_user";
    private static final String PASSWORD_KEY = "cql_password";
    private static final String CONTACT_POINTS_KEY = "contact_points";
    private static final String PORT_KEY = "port";
    private static final String CLUSTER_KEY = "name";
    private static final String DC_KEY = "dc";

    private String user;
    private String password;
    private String name;
    private String dc;
    private String[] contactPoints;
    private int port;

    public Cluster getCluster() {
        return newCluster();
    }

    public void initialize(Map<String, String> conf, String identifier) {
        user = getEnv(identifier, USERNAME_KEY);
        password = getEnv(identifier, PASSWORD_KEY);
        name = conf.get(CLUSTER_KEY);
        dc = conf.get(DC_KEY);
        contactPoints = conf.get(CONTACT_POINTS_KEY).split(",");
        port = Integer.parseInt(conf.getOrDefault(PORT_KEY, "9042"));
    }

    private synchronized Cluster newCluster() {
        // TODO add policies etc
        Cluster.Builder builder = Cluster.builder()
                                         .addContactPoints(contactPoints)
                                         .withPort(port);

        if (user != null)
            builder.withCredentials(user, password);

        return builder.build();
    }

    public String getClusterName() {
        return name;
    }

    public String toString() {
        return String.format("ContactPoints Cluster: name=%s, dc=%s, contact points= [%s]",
                             "name", dc, String.join(",'", contactPoints));
    }

    static String getEnv(String identifier, String propName) {
        return System.getenv(String.format("%s.%s.%s", PREFIX, identifier, propName));
    }

}
