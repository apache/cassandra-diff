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

package org.apache.cassandra.diff.api.resources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Health Check: not terribly useful yet.
 * TODO: add DB connection check
 */
@Path("/__health")
public class HealthResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(HealthResource.class);

  private static Boolean overrideHealthStatus = null;

  public HealthResource() {
    LOGGER.debug("New HealthResource created.");
  }

  public static Boolean overrideHealthStatusTo(Boolean newStatus) {
    Boolean oldStatus = overrideHealthStatus;
    overrideHealthStatus = newStatus;
    LOGGER.debug("Changed overrideHealthStatus from {} to {}", oldStatus, overrideHealthStatus);
    return oldStatus;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response health() {
    if (isHealthy()) {
      return Response
          .ok("All is good.")
          .build();
    } else {
      return Response
          .serverError()
          .entity("Not Healthy.")
          .build();
    }
  }

  private boolean isHealthy() {
    if (overrideHealthStatus == null) {
      return true;
    }
    return overrideHealthStatus;
  }

}
