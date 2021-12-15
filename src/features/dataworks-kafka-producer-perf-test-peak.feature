@dataworks-kafka-producer-perf-test-peak
Feature: Performance test scenarios for dataworks msk kafka producer application
  Scenario: Performance test for a kafka producer application using 1,000 messages. Messages/file 1,000.
    Given '1' encrypted json file(s) with '1000' messages are available in S3 location
    When the kafka producer app is started
    Then the kafka topic should have '1000' messages

  Scenario: Performance test for a kafka producer application using 10,000 messages. Messages/file 10,000.
    Given '1' encrypted json file(s) with '10000' messages are available in S3 location
    When the kafka producer app is started
    Then the kafka topic should have '10000' messages

  Scenario: Performance test for a kafka producer application using 10,000 messages. Messages/file 100,000.
    Given '1' encrypted json file(s) with '100000' messages are available in S3 location
    When the kafka producer app is started
    Then the kafka topic should have '100000' messages
