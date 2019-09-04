# Apache Cassandra diff

## Configuration
See `spark-job/localconfig.yaml` for an example config.

## Custom cluster providers
To make it easy to run in any environment the cluster providers are pluggable - there are two interfaces to implement.
First, the `ClusterProvider` interface is used to create a connection to the clusters, and it is configured using
`JobConfiguration#clusterConfig` (see below).
### cluster_config
This section has 3 parts - `source`, `target` and `metadata` where source and target describes the clusters that should
be compared and metadata describes where we store information about any mismatches and the progress the job has done. 
Metadata can be stored in one of the source/target clusters or in a separate cluster.

The fields under source/target/metadata are passed in to the `ClusterProvider` (described by `impl`) as a map, so any
custom cluster providers can be configured here.

## Setting up clusters for diff
One way of setting up clusters for diff is to restore a snapshot to two different clusters and then modifying one 
of the clusters to be able to make sure that the queries still return the same results. This could include 
upgrades/replacements/bounces/decommission/expansion. 

## Environment variables
Currently usernames and passwords are set as environment variables when running the diff tool and the api server:

* `diff.cluster.<identifier>.cql_user` - the user name to use
* `diff.cluster.<identifier>.cql_password` - password

where `<identifier>` should be `source`, `target` and `metadata` for the username/password combinations for the
matching clusters in the configuration.

## Example
This example starts two cassandra single-node clusters in docker, runs stress to populate them and then runs diff 
to make sure the data matches;

You need to have docker and spark setup.

```shell script
$ git clone <wherever>/cassandra-diff.git
$ cd cassandra-diff
$ mvn package
$ docker run --name cas-src -d  -p 9042:9042 cassandra:3.0.18
$ docker run --name cas-tgt -d  -p 9043:9042 cassandra:latest
$ docker exec cas-src cassandra-stress write n=1k
$ docker exec cas-tgt cassandra-stress write n=1k
$ spark-submit --verbose --files ./spark-job/localconfig.yaml --class org.apache.cassandra.diff.DiffJob spark-job/target/spark-job-0.1-SNAPSHOT.jar localconfig.yaml
# ... logs
INFO  DiffJob:124 - FINISHED: {standard1=Matched Partitions - 1000, Mismatched Partitions - 0, Partition Errors - 0, Partitions Only In Source - 0, Partitions Only In Target - 0, Skipped Partitions - 0, Matched Rows - 1000, Matched Values - 6000, Mismatched Values - 0 }
## start api-server:
$ mvn install
$ cd api-server
$ mvn exec:java
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
## Releases
We push maven artifacts to `repository.apache.org`. To create a release, follow the instructions
[here|http://www.apache.org/dev/publishing-maven-artifacts.html], basically:

1. make sure your `~/.m2/settings.xml` has entries for the apache repositories:
   ```xml
   <settings>
   ...
       <servers>
           <!-- To publish a snapshot of some part of Maven -->
           <server>
               <id>apache.snapshots.https</id>
               <username> <!-- YOUR APACHE LDAP USERNAME --> </username>
               <password> <!-- YOUR APACHE LDAP PASSWORD (encrypted) --> </password>
           </server>
           <!-- To stage a release of some part of Maven -->
           <server>
               <id>apache.releases.https</id>
               <username> <!-- YOUR APACHE LDAP USERNAME --> </username>
               <password> <!-- YOUR APACHE LDAP PASSWORD (encrypted) --> </password>
           </server>
           ...
       </servers>
    </settings>
   ```
2. Generate GPG keys:
  ```shell script
$ gpg --gen-key
  ```

3. Try a local install with the apache profile to make sure everything is setup correctly:
  ```shell script
$ mvn clean install -Papache-release
  ```
  Note, if you get an error like `gpg: signing failed: Inappropriate ioctl for device` you can run the command like
  this instead:
  ```shell script
$ GPG_TTY=$(tty) mvn clean install -Papache-release
  ```

4. Publish a snapshot:
  ```shell script
$ mvn deploy
  ```

5. Prepare the release:
  ```shell script
$ mvn release:clean
$ mvn release:prepare
  ```

6. Stage the release for a vote
  ```shell script
$ mvn release:perform
  ```