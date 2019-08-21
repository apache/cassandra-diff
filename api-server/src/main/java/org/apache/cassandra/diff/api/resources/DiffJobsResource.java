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

import java.util.SortedSet;
import java.util.UUID;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.diff.JobConfiguration;
import org.apache.cassandra.diff.api.services.DBService;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import static com.google.common.base.Strings.isNullOrEmpty;

@Path("/jobs")
public class DiffJobsResource {

    private static final Logger logger = LoggerFactory.getLogger(DiffJobsResource.class);
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern(DATE_FORMAT).withZoneUTC();
    private static final ObjectMapper MAPPER = new ObjectMapper().setVisibility(PropertyAccessor.FIELD,
                                                                                JsonAutoDetect.Visibility.NON_PRIVATE);
    private final DBService dbService;

    public DiffJobsResource(JobConfiguration conf) {
        dbService = new DBService(conf);
    }

    @GET
    @Path("/running/id")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRunningJobIds() {
        return response(dbService.fetchRunningJobs());
    }

    @GET
    @Path("/running")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRunningJobs() {
        return response(dbService.fetchJobSummaries(dbService.fetchRunningJobs()));
    }

    @GET
    @Path("/recent")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRecentJobs() {

        SortedSet<DBService.JobSummary> recentJobs = Sets.newTreeSet(DBService.JobSummary.COMPARATOR.reversed());
        DateTime now = DateTime.now(DateTimeZone.UTC);
        DateTime maxStartDate = now;
        DateTime minStartDate = maxStartDate.minusDays(30);
        recentJobs.addAll(dbService.fetchJobsStartedBetween(minStartDate, maxStartDate));

        while (recentJobs.size() < 10 && (maxStartDate.compareTo(now.minusDays(90)) >= 0)) {
            maxStartDate = minStartDate;
            minStartDate = minStartDate.minusDays(30);
            recentJobs.addAll(dbService.fetchJobsStartedBetween(minStartDate, maxStartDate));
        }

        return response(recentJobs);
    }

    @GET
    @Path("/{jobid}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJob(@PathParam("jobid") final String jobId) {
        return response(dbService.fetchJobSummary(UUID.fromString(jobId)));
    }

    @GET
    @Path("/{jobid}/results")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobResults(@PathParam("jobid") final String jobId) {
        return response(dbService.fetchJobResults(UUID.fromString(jobId)));
    }

    @GET
    @Path("/{jobid}/status")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobStatus(@PathParam("jobid") final String jobId) {
        return response(dbService.fetchJobStatus(UUID.fromString(jobId)));
    }

    @GET
    @Path("/{jobid}/mismatches")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobMismatches(@PathParam("jobid") final String jobId) {
        return response(dbService.fetchMismatches(UUID.fromString(jobId)));
    }

    @GET
    @Path("/{jobid}/errors/summary")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobErrorSummary(@PathParam("jobid") final String jobId) {
        return response(dbService.fetchErrorSummary(UUID.fromString(jobId)));
    }

    @GET
    @Path("/{jobid}/errors/ranges")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobErrorRanges(@PathParam("jobid") final String jobId) {
        return response(dbService.fetchErrorRanges(UUID.fromString(jobId)));
    }

    @GET
    @Path("/{jobid}/errors")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobErrorDetail(@PathParam("jobid") final String jobId) {
        return response(dbService.fetchErrorDetail(UUID.fromString(jobId)));
    }

    @GET
    @Path("/by-start-date/{started-after}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobsStartedSince(@PathParam("started-after") final String minStart) {

        DateTime minStartDate = parseDate(minStart);
        if (minStartDate == null)
            return Response.status(400).entity("Invalid date, please supply in the format yyyy-MM-dd").build();

        DateTime maxStartDate = DateTime.now(DateTimeZone.UTC);

        return getJobsStartedBetween(minStartDate, maxStartDate);
    }

    @GET
    @Path("/by-start-date/{started-after}/{started-before}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobsStartedSince(@PathParam("started-after") final String minStart,
                                        @PathParam("started-before") final String maxStart) {

        DateTime minStartDate = parseDate(minStart);
        if (minStartDate == null)
            return Response.status(400).entity("Invalid date, please supply in the format yyyy-MM-dd").build();

        DateTime maxStartDate = null;
        if (isNullOrEmpty(maxStart)) {
            DateTime.now(DateTimeZone.UTC);
        }
        else {
            maxStartDate = parseDate(maxStart);
            if (maxStartDate == null)
                return Response.status(400).entity("Invalid date, please supply in the format yyyy-MM-dd").build();
            if (maxStartDate.compareTo(minStartDate) < 0)
                return Response.status(400).entity("Invalid date range, started-before cannot be earlier than started-after").build();
        }

        return getJobsStartedBetween(minStartDate, maxStartDate);
    }

    @GET
    @Path("/by-source-cluster/{source}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobsBySourceCluster(@PathParam("source") final String sourceClusterName) {
        return response(dbService.fetchJobsForSourceCluster(sourceClusterName));
    }

    @GET
    @Path("/by-target-cluster/{target}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobsByTargetCluster(@PathParam("target") final String targetClusterName) {
        return response(dbService.fetchJobsForTargetCluster(targetClusterName));
    }

    @GET
    @Path("/by-keyspace/{keyspace}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobsByKeyspace(@PathParam("keyspace") final String keyspace) {
        return response(dbService.fetchJobsForKeyspace(keyspace));
    }

    private Response response(Object data) {
        try {
            return Response.ok().entity(MAPPER.writer().writeValueAsString(data)).build();
        }
        catch (JsonProcessingException e) {
            logger.error("JSON Processing error", e);
            return Response.serverError().entity("Error constructing JSON response").build();
        }
    }

    private Response getJobsStartedBetween(DateTime minStartDate, DateTime maxStartDate) {
        return response(dbService.fetchJobsStartedBetween(minStartDate, maxStartDate));
    }

    private DateTime parseDate(String s) {
        try {
            return DATE_FORMATTER.parseDateTime(s);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    public void close() {
        dbService.close();
    }
}
