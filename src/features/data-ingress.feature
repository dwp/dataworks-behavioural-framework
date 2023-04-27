# this test depends on resources in dataworks-aws-data-ingress pipeline (aws_autoscaling_schedule in
# data-ingress-test-scaling module) and will not run locally
@data-ingress
Feature: Data ingress cluster scaling schedules and SFT task with Trend Micro

  @fixture.s3.clear.ingress.sft.start
  @fixture.stop.data.ingress
  @fixture.delete.scheduled.action.di
  Scenario: The data ingress cluster detects test virus and sft receives file
    Given instances should start in '5' and stop in '18' min after the pipeline has run
    When instance starts within the expected time
    When sender agent task and receiver agent task run
    Then new trend micro test pass file is on s3
    Then new test file sent by sft sender is on s3
