@aws-clive
@test
Feature: Clive tests, to run clive and validate its output

  @fixture.terminate.clive.cluster
  Scenario: CLIVE dataset E2E given latest ADG output
    Given the results of the dynamodb table 'data_pipeline_metadata' for 'ADG-full'
    Then start the CLIVE cluster and wait for the step 'run-clive'
    And insert the 'hive-query' step onto the CLIVE cluster
    And wait '120' minutes for the step to finish
    Then the CLIVE result matches the expected results of 'core_contract_expected.csv'



