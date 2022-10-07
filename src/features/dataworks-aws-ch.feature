@dataworks-aws-ch
Feature: Ch etl to produce input data and verify correct output after processing
  @fixture.s3.clear.ch.start
  @fixture.terminate.ch.cluster
  Scenario: Cluster Ch completes and passes checks if the file conforms to expectations,
            while it fails and triggers alarms in the event of deviations from the expected format.
    When The cluster starts without steps
    Then Download and parse conf file
    Then Generate files having expected format and size to test positive outcome
    Then Upload the local file to s3
    Then Set the dynamo db bookmark on the first filename generated
    Then Add the etl step in e2e mode and wait for it to complete
    Then Add validation step and verify it completes
    Then Verify last imported file was updated on DynamoDB
    ### wrong size
    When The cluster is still running
    Then Clear S3 prefix where previous synthetic data is
    Then Generate files having wrong size for negative testing
    Then Upload the local file to s3
    Then Set the dynamo db bookmark on the first filename generated
    Then Add the etl step in e2e mode and wait for it to fail
    Then Verify that the alarms went on due to wrong file size
    ### one extra column
    Then Clear S3 prefix where previous synthetic data is
    Then Generate files having one extra column for negative testing
    Then Upload the local file to s3
    Then Set the dynamo db bookmark on the first filename generated
    Then Add the etl step in e2e mode and wait for it to fail
    Then Verify that the alarms went on due extra column
    ### one column less
    Then Clear S3 prefix where previous synthetic data is
    Then Generate files having one less column for negative testing
    Then Upload the local file to s3
    Then Set the dynamo db bookmark on the first filename generated
    Then Add the etl step in e2e mode and wait for it to fail
    Then Verify that the alarms went on due to wrong file size
    ### wrong headers
    Then Clear S3 prefix where previous synthetic data is
    Then Generate files having right number of columns with wrong headers for negative testing
    Then Upload the local file to s3
    Then Set the dynamo db bookmark on the first filename generated
    Then Add the etl step in e2e mode and wait for it to fail
    Then Verify that the alarms went on due to wrong headers
    ### row with one missing field
    Then Clear S3 prefix where previous synthetic data is
    Then Generate files having right columns, one row with one missing field for negative testing
    Then Upload the local file to s3
    Then Set the dynamo db bookmark on the first filename generated
    Then Add the etl step in e2e mode and wait for it to fail
    Then Verify that the alarms went on due to row with one missing field
    ### row with one numeric value
    Then Clear S3 prefix where previous synthetic data is
    Then Generate files having a row with numeric values for negative testing
    Then Upload the local file to s3
    Then Set the dynamo db bookmark on the first filename generated
    Then Add the etl step in e2e mode and wait for it to fail
    Then Verify that the alarms went on due to wrong data type
