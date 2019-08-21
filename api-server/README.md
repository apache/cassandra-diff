# Diff API Server
## Configuration
See main project README - the api server reads the `metadata_options` and `cluster_config.metadata` to connect

## Running locally
`mvn exec:java`

## Endpoints
### `/jobs/running/id`
Returns the ids of currently running jobs

### `/jobs/running`
Summaries of all currently running jobs

### `/jobs/recent`
Summaries of recently run jobs
Example:
```shell script
$ curl -s localhost:8089/jobs/recent | python -mjson.tool
  [
      {
          "jobId": "99b8d556-07ed-4bfd-b978-7d9b7b2cc21a",
          "buckets": 100,
          "keyspace": "keyspace1",
          "tables": [
              "standard1"
          ],
          "sourceClusterName": "local_test_1",
          "sourceClusterDesc": "ContactPoints Cluster: name=name, dc=datacenter1, contact points= [127.0.0.1]",
          "targetClusterName": "local_test_2",
          "targetClusterDesc": "ContactPoints Cluster: name=name, dc=datacenter1, contact points= [127.0.0.1]",
          "tasks": 10000,
          "start": "2019-08-16T11:47:36.123Z"
      }
  ]

```

### `/jobs/{jobid}`
Summary about a single job
Example:
```shell script
$ curl -s localhost:8089/jobs/99b8d556-07ed-4bfd-b978-7d9b7b2cc21a | python -mjson.tool
  {
      "jobId": "99b8d556-07ed-4bfd-b978-7d9b7b2cc21a",
      "buckets": 100,
      "keyspace": "keyspace1",
      "tables": [
          "standard1"
      ],
      "sourceClusterName": "local_test_1",
      "sourceClusterDesc": "ContactPoints Cluster: name=name, dc=datacenter1, contact points= [127.0.0.1]",
      "targetClusterName": "local_test_2",
      "targetClusterDesc": "ContactPoints Cluster: name=name, dc=datacenter1, contact points= [127.0.0.1]",
      "tasks": 10000,
      "start": "2019-08-16T11:47:36.123Z"
  }
```

### `/jobs/{jobid}/results`
The results for the given job.
Example:
```shell script
$ curl -s localhost:8089/jobs/99b8d556-07ed-4bfd-b978-7d9b7b2cc21a/results | python -mjson.tool
[
    {
        "jobId": "99b8d556-07ed-4bfd-b978-7d9b7b2cc21a",
        "table": "standard1",
        "matchedPartitions": 1000,
        "mismatchedPartitions": 0,
        "matchedRows": 1000,
        "matchedValues": 6000,
        "mismatchedValues": 0,
        "onlyInSource": 0,
        "onlyInTarget": 0,
        "skippedPartitions": 0
    }
]
```

### `/jobs/{jobid}/status`
Current status for a job, shows how many splits have been finished.
Example:
```shell script
$ curl -s localhost:8089/jobs/99b8d556-07ed-4bfd-b978-7d9b7b2cc21a/status | python -mjson.tool
{
    "jobId": "99b8d556-07ed-4bfd-b978-7d9b7b2cc21a",
    "completedByTable": {
        "standard1": 10000
    }
}
```

### `/jobs/{jobid}/mismatches`
Number of mismatches for the job.
```shell script
$  curl -s localhost:8089/jobs/99b8d556-07ed-4bfd-b978-7d9b7b2cc21a/mismatches | python -mjson.tool
  {
      "jobId": "99b8d556-07ed-4bfd-b978-7d9b7b2cc21a",
      "mismatchesByTable": {}
  }
```

### `/jobs/{jobid}/errors/summary`
Summary of the number errors for the job. 

### `/jobs/{jobid}/errors/ranges`
Lists failed ranges for the job.

### `/jobs/{jobid}/errors`
Details about the job errors.

### `/jobs/by-start-date/{started-after}`
### `/jobs/by-start-date/{started-after}/{started-before}`
### `/jobs/by-source-cluster/{source}`
### `/jobs/by-target-cluster/{target}`
### `/jobs/by-keyspace/{keyspace}`
