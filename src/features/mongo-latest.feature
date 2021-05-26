@mongo-latest
@test
Feature: Mongo latest end to end test

    @fixture.s3.clear.hive.query.output.start
    @fixture.s3.clear.mongo.latest.input.start
    @fixture.terminate.mongo_latest.cluster
    Scenario: Mongo latest end to end test
      Given I start the mongo latest cluster
      When I insert the 'hive-query' step onto the mongo latest cluster
      And insert the dynamodb check query step onto the mongo latest cluster
      And wait a maximum of '120' minutes for the last mongo latest step to finish
      Then the mongo latest result for step 'hive-query' matches the expected results of 'statement_fact_v_expected.csv'
      Then the mongo latest result for step 'dynamodb-table' matches the expected results of 'dynamo_db_hive_table_expected.csv'
      And the mongo latest cluster tags have been created correctly
      And the mongo latest metadata table is correct
