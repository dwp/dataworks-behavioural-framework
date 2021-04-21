@aws-clive
@test
Feature: Clive tests, to run clive and validate its output

  Scenario: Using ADG output data, the Clive process creates Hive tables on this data, that is queryable and contains the data of the ADG output files.
    Given the results of the dynamodb table 'data_pipeline_metadata' for 'ADG-full'
    When I start the CLIVE cluster and wait for the step 'run-clive'

