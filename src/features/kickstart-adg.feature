@aws-kickstart-adg
@test
Feature: Kickstart adg process, to source data and valid final tables for expected outcome

  @fixture.s3.clear.kickstart.start
  @fixture.terminate.kickstart.cluster
  Scenario: Kickstart adg end to end test for vacancy data
    Given The template file 'record_template.json' as an input, generate '10' records per table for 'vacancy' with PII flag is 'False' and upload to s3 bucket
    Then Start kickstart adg emr process for module 'vacancy' and wait for step 'submit-job' to complete
    And  Add steps 'hive-validation-queries' to kickstart adg emr cluster for 'vacancy' and wait for these steps to completed
    And The input result matches with final output for module 'vacancy'
