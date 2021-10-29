@dataworks-kafka-producer-perf-test-soak
Feature: Performance test scenarios for dataworks msk kafka producer application
  Scenario: Performance test for a kafka producer application using 50,000 messages. Messages/file 10,000.
    Given '5' encrypted json file(s) with '10000' messages are available in S3 location
    When the kafka producer app is started
    Then the kafka topic should have '50000' messages

  Scenario: Performance test for a kafka producer application using 100,000 messages. Messages/file 10,000.
    Given '10' encrypted json file(s) with '10000' messages are available in S3 location
    When the kafka producer app is started
    Then the kafka topic should have '100000' messages

  Scenario: Performance test for a kafka producer application using 500,000 messages. Messages/file 10,000.
    Given '50' encrypted json file(s) with '10000' messages are available in S3 location
    When the kafka producer app is started
    Then the kafka topic should have '500000' messages

  Scenario: Performance test for a kafka producer application using 500,000 messages. Messages/file 10,000.
    Given '100' encrypted json file(s) with '10000' messages are available in S3 location
    When the kafka producer app is started
    Then the kafka topic should have '1000000' messages

