@data-ingress
Feature: Data ingress cluster scaling schedules
  # this test depends on tf resources in dataworks-aws-data-ingress pipeline and will not run locally
  @fixture.s3.clear.ingress.sft.start
  Scenario: The data ingress cluster scales in response to the test aws_autoscaling_schedule tf resources
    Given the autoscaling schedules replicas that are set to scale up after '5' min and scale down after '18' min
    Then instance scales up within the expected time
    When run sender agent task and receiver agent task
    Then trend micro test pass file is present on s3
    Then test file sent by sft sender is present on s3
    Then instance to scale down within the expected time
