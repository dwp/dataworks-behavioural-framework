@dataworks-kafka-producer-app
Feature: Publish data in S3 bucket to kafka topic
  @fixture.init.e2e.dataworks.kafka.producer
  @fixture.stop.e2e.dataworks.kafka.producer
  Scenario: End-2-End scenario for a kafka producer application
    Given the e2e kafka producer app is running
    When a json file 'input-test-data.json.out' is uploaded to S3 location
    Then the last offset should be incremented by '6'
