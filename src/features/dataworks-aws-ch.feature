@dataworks-aws-ch
Feature: Synthesise source file, process them and check output tables

  @fixture.s3.clear.ch.start
  @fixture.terminate.ch.cluster
  Scenario: Cluster completes if the file conforms to size and format expectations; it fails and triggers alarms otherwise.
    ### file is as expected
    When The cluster starts without steps
    When Download and parse conf file
    When Generate files having expected format and size to test positive outcome
    When Zip and upload the local file to s3
    When Set the dynamo db bookmark on the first filename generated
    Then Etl step in e2e mode completes
    Then Validation step completes
    Then Last imported file is updated on DynamoDB
    ### one extra column
    When Clear S3 prefix where previous synthetic data is
    When Generate files having one extra column for negative testing
    When Zip and upload the local file to s3
    When Set the dynamo db bookmark on the first filename generated
    Then Etl step in e2e mode fails
    Then File format alarm triggers
    ### incorrect headers
    When Clear S3 prefix where previous synthetic data is
    When Generate files having incorrect headers for negative testing
    When Zip and upload the local file to s3
    When Set the dynamo db bookmark on the first filename generated
    Then Etl step in e2e mode fails
    Then File format alarm triggers
    ### row with one missing field
    When Clear S3 prefix where previous synthetic data is
    When Generate files having a row with one missing field for negative testing
    When Zip and upload the local file to s3
    When Set the dynamo db bookmark on the first filename generated
    Then Etl step in e2e mode fails
    Then File format alarm triggers
    ### row with string value where it should be int according to schema
    When Clear S3 prefix where previous synthetic data is
    When Generate files having a row with string values instead of int
    When Zip and upload the local file to s3
    When Set the dynamo db bookmark on the first filename generated
    Then Etl step in e2e mode fails
    Then File format alarm triggers
