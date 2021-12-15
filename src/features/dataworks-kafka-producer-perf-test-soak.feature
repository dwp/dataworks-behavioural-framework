@dataworks-kafka-producer-perf-test-soak
Feature: Performance test scenarios for dataworks msk kafka producer application
  Scenario: Performance test for a kafka producer application using 100,000 messages. Messages/file 10,000. Set1.
    Given '10' encrypted json file(s) with '10000' messages are available in S3 location
    When the kafka producer app is started
    Then the kafka topic should have '100000' messages

  Scenario: Performance test for a kafka producer application using 100,000 messages. Messages/file 10,000. Set2.
    Given '10' encrypted json file(s) with '10000' messages are available in S3 location
    When the kafka producer app is started
    Then the kafka topic should have '100000' messages

  Scenario: Performance test for a kafka producer application using 100,000 messages. Messages/file 10,000. Set3.
    Given '10' encrypted json file(s) with '10000' messages are available in S3 location
    When the kafka producer app is started
    Then the kafka topic should have '100000' messages

  Scenario: Performance test for a kafka producer application using 100,000 messages. Messages/file 10,000. Set4.
    Given '10' encrypted json file(s) with '10000' messages are available in S3 location
    When the kafka producer app is started
    Then the kafka topic should have '100000' messages
