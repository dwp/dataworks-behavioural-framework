@aws-kickstart-adg-data-generator
@test
Feature: Generate data for kickstart adg process

  @fixture.s3.clear.kickstart.start
  Scenario: Generate test data for kickstart vacancy adg process
    Given The template file 'record_template.json' as an input, generate '100' records per table for 'vacancy'
    Then upload the local files to s3 bucket in 'unencrypted' format

  Scenario: Generate test data for kickstart grant application adg process
    Given The template file 'record_template.json' as an input, generate '100' records per table for 'application'
    Then upload the local files to s3 bucket in 'encrypted' format

  Scenario: Generate test data for kickstart payment adg process
    Given The template file 'record_template.json' as an input, generate '100' records per table for 'payment'
    Then upload the local files to s3 bucket in 'encrypted' format

