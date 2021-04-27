@clive
@test
Feature: Clive tests, to run clive and validate its output

  @fixture.s3.clear.clive.start
  @fixture.terminate.clive.cluster
  @fixture.s3.clear.published.bucket.clive.test.input
  Scenario: CLIVE dataset E2E given latest ADG output
    Given ADG output 'adg_output' as an input data source on S3
    Then start the CLIVE cluster and wait for the step 'run-clive' for '180'
    And insert the 'hive-query' step onto the CLIVE cluster
    And wait '120' minutes for the step to finish
    Then the CLIVE result matches the expected results of 'core_contract_expected.csv'



