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

package org.apache.cassandra.diff.api;

import java.io.FileInputStream;
import java.io.IOException;

import com.google.common.collect.Lists;

import org.apache.cassandra.diff.YamlJobConfiguration;
import org.apache.cassandra.diff.api.resources.DiffJobsResource;
import org.apache.cassandra.diff.api.resources.HealthResource;
import org.apache.cxf.endpoint.Server;
import org.apache.cxf.jaxrs.JAXRSServerFactoryBean;
import org.apache.cxf.jaxrs.lifecycle.SingletonResourceProvider;

public class DiffAPIServer {
    public static void main(String[] args) throws IOException {
        String filename = args[0];
        JAXRSServerFactoryBean factoryBean = new JAXRSServerFactoryBean();

        DiffJobsResource diffResource = new DiffJobsResource(YamlJobConfiguration.load(new FileInputStream(filename)));
        factoryBean.setResourceProviders(Lists.newArrayList(new SingletonResourceProvider(diffResource),
                                                            new SingletonResourceProvider(new HealthResource())));
        factoryBean.setAddress("http://localhost:8089/");
        Server server = factoryBean.create();

        try {
            server.start();
            System.in.read();
        } catch (Throwable t) {
            t.printStackTrace(System.out);
            throw t;
        } finally {
            diffResource.close();
            if (server.isStarted())
                server.stop();
            System.exit(0);
        }
    }
}
