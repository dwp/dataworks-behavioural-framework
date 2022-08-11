@dataworks-aws-ch
Feature: Ch etl to produce input data and verify correct output after processing
  @fixture.s3.clear.ch.start
  @fixture.terminate.ch.cluster
  Scenario: Ch cluster end to end test
    When The cluster starts without steps
    Then Download the file that includes the etl arguments from s3 and parse it
    Then Generate '3' files each with '7' rows
    Then Upload the local files to s3
    Then Set the dynamo db bookmark on the first filename generated
    Then Add the etl step in e2e mode and wait for it to complete
    Then Add validation step and verify it completes
    Then Verify last imported file was updated on DynamoDB
