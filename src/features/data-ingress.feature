@data-ingress
Feature: Data ingress e2e
  @fixture.s3.clear.data.ingress.start
  Scenario: Trend micro uses remediation action on test file in the sft container
    When data-ingress service restarts
    Then Wait for pass file indicating that test virus file was correctly detected
