@data-ingress
Feature: Data ingress cluster scaling schedules
  # this test depends on tf resources in dataworks-aws-data-ingress pipeline and will not run locally
  @fixture.stop.data.ingress
  @fixture.s3.clear.ingress.sft.start
  Scenario: The data ingress cluster scales in response to the test aws_autoscaling_schedule tf resources
    Given the instance is set to start after '5' min and stop after '22' min
    Then instance starts within the expected time
    When sender agent task and receiver agent task run
    Then new trend micro test pass file is on s3
    Then new test file sent by sft sender is on s3
    Then instance stops within the expected time
