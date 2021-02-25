# Dataworks AWS Manifest Comparison

This page details the manifest comparison process which is used for data reconciliation in the ingestion components between the Kafka stream and the imported data dump.

## Data reconciliation

The data reconciliation process compares the individual records across the timespan that have been imported from the data dump to the individual records that are exported to Crown. This provides very high confidence in the data quality and that no records are missing from the Kafka stream or have gone missing during the ingestion process.

The data reconciliation works together with the rest of the [AWS DataWorks Test Strategy](aws_dataworks_test_strategy.md) to provide high levels of confidence in the overall product and data quality.

## Manifest creation

There are two manifests that are produced:

1. The Historic Data Importer (HDI) produces a manifest when it is run in "Manifest mode", which is a switch in the message that kicks it off
2. The HBase-to-Mongo Exporter (HTME) produces a manfest every time it runs

### Contents

The manifests contains the following:

* One row in a CSV format for each record:
* * For HDI, this is every valid record that is decrypted and imported
* * For HTME, this is every valid record that is written to a snapshot file successfully
* Each row contains the following information for a record:
* * The "id" field for the record which is used as HBase record key, pre-formatted
* * The timestamp that is against the record in epoch format
* * The database the record is from
* * The collection the record is from
* * The type of record (i.e. IMPORT or EXPORT)
* * The source of the record (either "HDI" or the "@type" field from Kafka messages)

### Test data

In development and QA environments, static manifest data is used. This data is in the repository in the `src/manifest-comparison/fixture-data` folder. It contains two import manifest files and two export ones and attempts to generate data scenarios for all the report scenarios that exist.

This data is uploaded to S3 in the concourse pipeline for this repository called `pull-request-dataworks-behavioural-framework` after PRs merge successfully.

## CI

There is a concourse pipeline called "manifest-comparison" which compares the latest versions of the two manifest file sets. The code for the pipeline is in the aws-ingestion repository.

The pipeline runs through dev and qa first with a set of test data, produces a manifest against this static data and then checks the results against expected values.

Then it runs in production against the real manifest data and generates a report.

This pipeline is automatically kicked off when any code is changed in this repository in the `src/manifest-comparison` folder only. It can be kicked off manually when required also.

## Manifest comparison code

The manifest comparison is a feature in this E2E repo. This is the feature that the CI pipeline executes when it runs.

### Steps 

There are a number of steps that the manifest comparison feature executes.

#### Before

Before the test runs, any local temporary locations or S3 locations for query outcomes are cleared.

#### Given The manifest generation tables have been created

This step executes three SQL scripts against [AWS Athena](https://aws.amazon.com/athena/):

1. drop-table.sql -> this runs twice and drops any existing tables for both import data and export data in order to start with a fresh table each time, this allows for schema changes
2. create-table.sql -> this creates two empty externals tables in Athena to represent the manifest data from the import set and the manifest data from the export set. The columns match the data in the files as described above.
3. create-parquet-table.sql -> this creates a big table with extra columns in [AWS Glue](https://aws.amazon.com/glue/) which will be populated later on. This table is in a columnar format called Parquet which is easier for large scale data analysis to be performed on.

#### And The manifest generation tables have been populated

This steps executes the AWS Glue job for the manifest etl. This job is deployed by the aws-ingestion repository and is a hive job running pyspark functions. This glue job uses the non parquet tables created earlier to quickly read the manifest files in the import and export s3 locations (which are passed in by CI to the comparison). It then performs logic decisions and data manipulations to work out the majority of the outcomes needed for the manifest comparison report. It uses the outcomes to populate the parquet table created in the first step.

#### When I generate the manifest comparison queries

This step generates temporary queries locally which will run in [AWS Athena](https://aws.amazon.com/athena/). These queries are executed against the now populated parquet table and will use simple select statements to produce their output. As the [AWS Glue](https://aws.amazon.com/glue/) job does the majority of the business logic, this enables these queries to be fairly simple. This is important so that [AWS Athena](https://aws.amazon.com/athena/) does not run out of resources when running the queries or time out.

The queries are written as templates which are modified at runtime with specific passed in data from CI and then written as temporary queries locally.

#### And I run the manifest comparison queries and upload the result to S3

This step executes the locally generated queries and the result is uploaded to S3 (location passed in by S3). The query results are also analysed by the code and formatted in to a report. This report is also saved as a local file and the report is uploaded to another location on S3 (this location is NOT cleared by the tests and so reports are stored indefinitely).

Each query has three files locally:

1. The template SQL file
2. A json file which provides data about the query, this is useful for the report and standardising output
3. A CSV file with expected results for the dev and qa environments (see the step below)

As well as uploading the report to S3, it is printed to stdout as well.

#### Then The query results match the expected results

On the development and QA environments, this step checks the outcome of each query against a local set of CSV files. Because the data in dev and QA is static and uploaded by the CI pipeline from this repository, then the queries in these environments have known outcomes. This step helps to verify the manifest comparison itself is working correctly.

On integration, preprod and prod, this step does nothing. This is achieved by an override environment variable passed in by CI telling it not to do anything here.

### Failures

If any query fails to execute, this will not stop the execution of the other queries. This is useful for debugging and seeing all issues upfront. Instead, the query execution step will complete, then it will log all the ones that failed and exit at that point with a failure.

If the expected results step has an assertion mismatch the run will fail (only applicable in dev and qa).
