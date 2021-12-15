@uc-feature
@test
Feature: uc-feature tests, to run uc-feature and validate its output
  @fixture.s3.clear.uc.feature.output
  @fixture.terminate.uc.feature.cluster
  Scenario: UC-FEATURE dataset E2E given latest ADG output
    Given I start the UC-FEATURE cluster
    Then I insert the 'hive-query' step onto the UC-FEATURE cluster
    And I wait '120' minutes for uc-feature hive-query to finish
    Then the UC-FEATURE result matches the expected results of 'mandatory_reconsideration_plus_json_expected.csv'
    And I check that the UC-FEATURE cluster tags have been created correctly
    And I check the UC-FEATURE metadata table is correct
    And the 'uc_feature' object_tagger has run successfully
    Then the correct tags are applied to the 'uc_feature' data
