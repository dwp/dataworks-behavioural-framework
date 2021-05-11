@ucfs-claimant-api
@fixture.claimant.api.setup
@fixture.ucfs.claimant.kafka.consumer.start
@test
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

  Scenario: A claimant with a single assessment period which ended in the last month is created
    Given I create a data file of 'single_new_claimant_with_given_thp.yml' for a claimant with a take home pay of '542.89' with a recently ended assessment period
    When UCFS send claimant API kafka messages with input file of 'valid_file_input.json' and data file of 'single_new_claimant_with_given_thp.yml'
    And The new claimants can be found from claimant API 'v2'
    And I query for the first new claimant from claimant API 'v2'
    And The query succeeds and returns that the claimant has been found