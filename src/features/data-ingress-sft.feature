@data-ingress-sft
Feature: Data ingress sft ingress agent
  @fixture.s3.clear.ingress.sft.start
  Scenario: Sft receives data and places it on the s3 bucket
    Given two instances are available for placing tasks in the ecs cluster
    Then run sender agent task to send test data and receiver agent task
    Then check if the test file is in s3
    Then reset the desired and max instance count in the asg to 0
