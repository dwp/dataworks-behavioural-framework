@aws-clive
@test
Feature: Clive tests, to run clive and validate its output

  @fixture.terminate.clive.cluster
  @fixture.s3.clear.published.bucket.clive.test.input
  @fixture.s3.clear.published.bucket.clive.test.output
  Scenario: Using ADG output data, the Clive process creates Hive tables on this data, that is queryable and contains the data of the ADG output files.
    When I start the CLIVE cluster

