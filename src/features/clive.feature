@aws-clive
@test
Feature: Clive tests, to run clive and validate its output

  @fixture.terminate.clive.cluster
  Scenario: Using ADG output data, the Clive process creates Hive tables on this data, that is queryable and contains the data of the ADG output files.
    Given the results of the dynamodb table 'data_pipeline_metadata' for 'ADG-full'
    Then start the CLIVE cluster and wait for the step 'create-clive-databases'
    And insert the 'hive-query' step onto the CLIVE cluster
    And wait '10' minutes for the step to finish
    Then the CLIVE result matches the expected results of 'core_contract_expected.csv'



