@mongo-latest
@test
Feature: Mongo latest end to end test

    @fixture.terminate.mongo_latest.cluster
    Scenario: Mongo latest end to end test
      Given I start the mongo latest cluster
      When I insert the 'hive-query' step onto the mongo latest cluster
      And wait a maximum of '60' minutes for the mongo latest step to finish
      Then the mongo latest result matches the expected results of 'statement_fact_v_expected.csv'
      And the mongo latest cluster tags have been created correctly
      And the mongo latest metadata table is correct
