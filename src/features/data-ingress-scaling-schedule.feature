@data-ingress-scaling
Feature: Data ingress cluster scaling schedules
  # this test depends on tf resources in dataworks-aws-data-ingress pipeline and will not run locally
  Scenario: The data ingress cluster scales in response to the test aws_autoscaling_schedule tf resources
    Given the autoscaling schedules replicas that are set to scale up after '7' min and scale down after '12' min
    Then wait for the instance to scale up within the expected time
    Then wait for the instance to scale down within the expected time
