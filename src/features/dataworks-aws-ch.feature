@dataworks-aws-ch
Feature: Synthesise source file, process them and check output tables

  @fixture.s3.clear.ch.start
  @fixture.terminate.ch.cluster
  Scenario: Cluster completes if the file conforms to size and format expectations; it fails and triggers alarms otherwise.
    ### file is as expected
    When The cluster starts without steps
    Then Download and parse conf file
    Then Generate files having expected format and size to test positive outcome
    Then Upload the local file to s3
    Then Set the dynamo db bookmark on the first filename generated
    Then Add the etl step in e2e mode and wait for it to complete
    Then Add validation step and verify it completes
    Then Verify last imported file was updated on DynamoDB
    ### wrong size
    Then Clear S3 prefix where previous synthetic data is
    Then Generate files having wrong size for negative testing
    Then Upload the local file to s3
    Then Set the dynamo db bookmark on the first filename generated
    Then Add the etl step in e2e mode and wait for it to fail
    Then Verify that the alarms turned on due to wrong file size
    ### one extra column
    Then Clear S3 prefix where previous synthetic data is
    Then Generate files having one extra column for negative testing
    Then Upload the local file to s3
    Then Set the dynamo db bookmark on the first filename generated
    Then Add the etl step in e2e mode and wait for it to fail
    Then Verify that the alarms turned on due to wrong file format
    ### one column less
    Then Clear S3 prefix where previous synthetic data is
    Then Generate files having one less column for negative testing
    Then Upload the local file to s3
    Then Set the dynamo db bookmark on the first filename generated
    Then Add the etl step in e2e mode and wait for it to fail
    Then Verify that the alarms turned on due to wrong file format
    ### wrong headers
    Then Clear S3 prefix where previous synthetic data is
    Then Generate files having right number of columns with wrong headers for negative testing
    Then Upload the local file to s3
    Then Set the dynamo db bookmark on the first filename generated
    Then Add the etl step in e2e mode and wait for it to fail
    Then Verify that the alarms turned on due to wrong file format
    ### row with one missing field
    Then Clear S3 prefix where previous synthetic data is
    Then Generate files having a row with one missing field for negative testing
    Then Upload the local file to s3
    Then Set the dynamo db bookmark on the first filename generated
    Then Add the etl step in e2e mode and wait for it to fail
    Then Verify that the alarms turned on due to wrong file format
    ### row with string value where it should be int according to schema
    Then Clear S3 prefix where previous synthetic data is
    Then Generate files having a row with string values for negative testing
    Then Upload the local file to s3
    Then Set the dynamo db bookmark on the first filename generated
    Then Add the etl step in e2e mode and wait for it to fail
    Then Verify that the alarms tuned on due to wrong file format
