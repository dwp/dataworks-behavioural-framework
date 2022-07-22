@datsci
@test
Feature: Datsci cluster test
  @fixture.terminate.datsci.cluster
  Scenario: Datsci cluster test
    Given I start the DATSCI cluster
    Then I insert the 'hive-select' step onto the DATSCI cluster
    And I wait '20' minutes
    Then I check that the DATSCI cluster tags have been created correctly
