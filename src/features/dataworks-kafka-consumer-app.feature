@dataworks-kafka-consumer-app
Feature: Publish DLQ data to S3 bucket from kafka DLQ topic
  @fixture.init.e2e.dataworks.kafka.consumer
  @fixture.stop.e2e.dataworks.kafka.consumer
  Scenario: End-2-End scenario for a kafka consumer application
    Given the e2e kafka consumer app is running
    When messages are published into the dlq topic using test data in 'input-test-data.json'
    Then the consumer should write '3' files to the S3 bucket
