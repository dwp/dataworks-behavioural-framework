@cyi
@test
Feature: CYI tests, to run CYI and validate its output
  @fixture.s3.clear.cyi.input
  @fixture.s3.clear.cyi.output
  @fixture.s3.clear.cyi.test.output
  @fixture.terminate.cyi.cluster
  Scenario: CYI dataset E2E given latest ADG output
    Given I upload a CYI file
    And I wait '10' minutes for a CYI cluster to start
    When I insert the 'hive-query' step onto the CYI cluster
    And I get the CYI Correlation Id from the tags
    And I wait '30' minutes for the CYI hive-query to finish
    Then the CYI result matches the expected results of 'cyi_managed_expected1.csv' and 'cyi_managed_expected2.csv'
    And I check the CYI metadata table is correct
