@aws-pdm-dataset-generation
@test
Feature: PDM data set generation tests, to source data and valid final tables for expected outcome

  @fixture.s3.clear.pdm.start
  @fixture.terminate.pdm.cluster
  @fixture.s3.clear.published.bucket.pdm.test.input
  @fixture.s3.clear.published.bucket.pdm.test.output
  Scenario: Using ADG output data, the PDM dataset generation process creates Hive tables on this data, that is queryable and contains the data of the ADG output files.
    Given the ADG uncompressed output 'agent-core/youthObligationDetails/youth_obligation_details.json' as an input data source on S3
    When I start the PDM cluster
    And insert the 'hive-query' step onto the cluster
    And the PDM cluster tags have been created correctly
    And wait a maximum of '120' minutes for the last step to finish
    Then the PDM result matches the expected results of 'youth_obligation_model_results.csv'
    And the PDM metadata table is correct
    And the pdm_object_tagger has run successfully
    And the correct tags are applied to the pdm data
