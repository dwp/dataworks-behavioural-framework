@ucfs-claimant-api
@fixture.claimant.api.setup
@fixture.ucfs.claimant.kafka.consumer.start
@test
@work-in-progress
Feature: UCFS Claimant API

  Scenario: Querying API for non existing claimant
    When I query for a claimant from claimant API 'v2' who does not exist
    Then The query succeeds and returns that the claimant has not been found

  Scenario: Take home pay can be decoded when querying for new claimant
    Given UCFS send claimant API kafka messages with input file of 'valid_file_input.json' and data file of 'single_new_claimant.yml'
    And The new claimants can be found from claimant API 'v2'
    When I query for the first new claimant from claimant API 'v2'
    Then Take home pay can be successfully decoded as '123.45'

  Scenario: Claimant is found when querying for new claimant
    Given UCFS send claimant API kafka messages with input file of 'valid_file_input.json' and data file of 'single_new_claimant.yml'
    And The new claimants can be found from claimant API 'v2'
    When I query for the first new claimant from claimant API 'v2'
    Then The query succeeds and returns that the claimant has been found

  Scenario: Assessment periods are returned when querying for new claimant
    Given UCFS send claimant API kafka messages with input file of 'valid_file_input.json' and data file of 'multiple_assessment_periods.yml'
    And The new claimants can be found from claimant API 'v2'
    When I query for the first new claimant from claimant API 'v2'
    Then The assessment periods are correctly returned using data file of 'multiple_assessment_periods.yml'

  Scenario: An existing claimant is suspended
    Given UCFS send claimant API kafka messages with input file of 'valid_file_input.json' and data file of 'single_new_claimant.yml'
    And The new claimants can be found from claimant API 'v2'
    When I query for the first new claimant from claimant API 'v2'
    And The query succeeds and returns that the claimant is not suspended
    And UCFS send kafka updates for first existing claimant with input file of 'valid_file_input.json' and data file of 'suspended_claimant.yml'
    Then I query the first claimant again from claimant API 'v2' and it is suspended

  Scenario: Malformed kafka data is sent to the DLQ
    Given UCFS send claimant API kafka messages with input file of 'invalid_file_input.json' and data file of 'single_new_claimant.yml'
    Then The messages are sent to the DLQ
    And I query for the first new claimant from claimant API 'v2'
    And The query succeeds and returns that the claimant has not been found

  Scenario: An existing claimant is deleted
    Given UCFS send claimant API kafka messages with input file of 'valid_file_input.json' and data file of 'single_new_claimant.yml'
    And The new claimants can be found from claimant API 'v2'
    When I query for the first new claimant from claimant API 'v2'
    And The query succeeds and returns that the claimant has been found
    And UCFS send a kafka delete for first existing claimant with input file of 'valid_delete.json'
    Then I query the first claimant again from claimant API 'v2' and it is not found

#  UC Test Scenarios - Any change of the particular values must be first agreed with them.
#  The output of these tests is provided to UC for testing in the integration environment.
  Scenario Outline: A claimant with assessment period(s), with the latest having ended in the last month, is created with given take home pay
    Given I create a data file of 'claimant_given_thp.yml' for a claimant with multiple assessment periods, with take home pay values of '<take-home-pay>'
    When UCFS send claimant API kafka messages with input file of 'valid_file_input.json' and data file of 'claimant_given_thp.yml'
    And The new claimants can be found from claimant API 'v2'
    And I query for the first new claimant from claimant API 'v2'
    Then Take home pay can be successfully decoded as '<take-home-pay>'
    And I print out the NINO for manual regression testing usage
    And I clean up the 'claimant_given_thp.yml' temporary files
    Examples:
    | take-home-pay            |
    | 542.89                   |
    | 542.87                   |
    | 542.88                   |
    | 0.0                      |
    | 601.88, 433.32           |
    | 601.88, 433.32, 742.89   |
    | 123.45, 123.45, 123.45   |
    | 0, 0, 0                  |