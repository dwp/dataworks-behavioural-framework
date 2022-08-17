@data-ingress
Feature: Data ingress components

  Scenario: Data ingress autoscaling schedules
#   warning: this test is not meant to run locally as it assumes a pipeline refresh
    Given the autoscaling schedules replicas that are set to scale up after 10 min and scale down after 15 min
    Then wait for the instance to scale up within the expected time
    Then wait for the instance to scale down within the expected time
