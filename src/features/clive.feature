@clive
@test
Feature: Clive tests, to run clive and validate its output
  @fixture.s3.clear.clive.output
  @fixture.terminate.clive.cluster
  Scenario: CLIVE dataset E2E given latest ADG output
    Given I start the CLIVE cluster and wait for the step 'run-clive' for '200' minutes
    Then I insert the 'hive-query' step onto the CLIVE cluster
    And I wait '120' minutes for the step to finish
    Then the CLIVE result matches the expected results of 'core_contract_expected.csv'
    And I check that the CLIVE cluster tags have been created correctly



