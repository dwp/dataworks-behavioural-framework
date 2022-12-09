@data-ingress
Feature: Data ingress cluster scaling schedules
  # this test depends on resources in dataworks-aws-data-ingress pipeline (aws_autoscaling_schedule in data-ingress-test-scaling module) and will not run locally
  @fixture.stop.data.ingress
  @fixture.delete.scheduled.action.di
  @fixture.s3.clear.ingress.sft.start
  Scenario: The data ingress cluster scales due to the autoscaling schedules, detects test virus, sft receives file
    Given that the instance should start '5' min and stop '18' min after the pipeline has run
    Then instance starts within the expected time
    When sender agent task and receiver agent task run
    Then new trend micro test pass file is on s3
    Then new test file sent by sft sender is on s3
    Then instance stops within the expected time
