@aws-clive
@test
Feature: Clive tests, to run clive and validate its output

  @fixture.s3.clear.clive.start
  @fixture.terminate.clive.cluster
  @fixture.s3.clear.published.bucket.clive.test.input
  @fixture.s3.clear.published.bucket.clive.test.output
  Scenario: Using ADG output data, the Clive process creates Hive tables on this data, that is queryable and contains the data of the ADG output files.
    When I start the CLIVE cluster
    And insert the 'hive-query' step onto the cluster
    And the CLIVE cluster tags have been created correctly
    And wait a maximum of '120' minutes for the step to finish
    Then the CLIVE result matches the expected results of 'youth_obligation_model_results.csv'
    And the CLIVE metadata table is correct
