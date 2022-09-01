@data-ingress
Feature: Data ingress components

  Scenario: Data ingress autoscaling schedules
#   warning: this test is not meant to run locally as it assumes a pipeline refresh
    Given the autoscaling schedules replicas that are set to scale up after 10 min and scale down after 15 min
    Then wait for the instance to scale up within the expected time
    Then wait for the instance to scale down within the expected time

  Scenario: Sft receives data and place it on the designated s3 bucket
    Given an sft agent task that sends the data to a receiver task running on a different instance
    Then set the desired and max instance count in the asg to 2 and refresh the sender and receiver services
    Then check if the test file ends up in s3 and the renaming is done correctly
