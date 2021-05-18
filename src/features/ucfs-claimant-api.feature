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

  Scenario Outline: Passported benefits regression scenarios (not suspended)
    Given UCFS send claimant API kafka messages with input file of 'valid_file_input.json' and data file of '<data-file>'
    And The new claimants can be found from claimant API 'v2'
    When I query for the first new claimant from claimant API 'v2'
    Then Take home pay can be successfully decoded as '<take-home-pay>'
    Examples:
    | data-file                                      | take-home-pay |
    | passported_benefits_regression_scenario_1.yml  | 542.89        |
    | passported_benefits_regression_scenario_2.yml  | 542.87        |
    | passported_benefits_regression_scenario_3.yml  | 542.88        |
    | passported_benefits_regression_scenario_4.yml  | 0.0           |
    | passported_benefits_regression_scenario_11.yml | 123.45        |

  Scenario Outline: Passported benefits regression scenarios (multi-assessment periods)
    Given UCFS send claimant API kafka messages with input file of 'valid_file_input.json' and data file of '<data-file>'
    And The new claimants can be found from claimant API 'v2'
    When I query for the first new claimant from claimant API 'v2'
    Then The assessment periods are correctly returned using data file of '<data-file>'
    Examples:
    | data-file                                      |
    | passported_benefits_regression_scenario_7.yml  |
    | passported_benefits_regression_scenario_8.yml  |
    | passported_benefits_regression_scenario_9.yml  |

  Scenario Outline: Passported benefits regression scenarios when querying with dates (multi-assessment periods)
    Given UCFS send claimant API kafka messages with input file of 'valid_file_input.json' and data file of '<data-file>'
    And The new claimants can be found from claimant API 'v2'
    When I query for the first claimant from claimant API 'v2' with the parameters file of '<parameters-file>'
    Then The assessment periods are correctly returned using data file of '<output-file>'
    Examples:
    | data-file                                      |  parameters-file                                 |  output-file                                                |
    | passported_benefits_regression_scenario_10.yml |  passported_benefits_regression_scenario_10.yml  |  passported_benefits_regression_scenario_10_last_month.yml  |

  Scenario Outline: Passported benefits regression scenarios (suspended)
    Given UCFS send claimant API kafka messages with input file of 'valid_file_input.json' and data file of '<data-file>'
    And The new claimants can be found from claimant API 'v2'
    When I query for the first new claimant from claimant API 'v2'
    Then Take home pay can be successfully decoded as '<take-home-pay>'
    And I query the first claimant again from claimant API 'v2' and it is suspended
    Examples:
    | data-file                                      | take-home-pay |
    | passported_benefits_regression_scenario_5.yml  | 783.99        |
    | passported_benefits_regression_scenario_6.yml  | 699.99        |

  Scenario Outline: Passported benefits regression scenarios (claim closed)
    Given UCFS send claimant API kafka messages with input file of 'valid_file_input.json' and data file of '<data-file>'
    And The new claimants can be found from claimant API 'v2'
    When I query for the first new claimant from claimant API 'v2'
    Then Take home pay can be successfully decoded as '<take-home-pay>'
    Examples:
      | data-file                                       | take-home-pay |
      | passported_benefits_regression_scenario_12.yml  | 123.45        |
