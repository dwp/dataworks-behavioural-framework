@data-egress-s3
Feature: Data egress transfer data to S3 end to end test

  Scenario: Data Egress service to transfer data to S3 end to end test
    Given the data in file 'data_egress_sample_data.txt' encrypted using DKS and uploaded to S3 bucket
    Then verify content of the data egress output file
    Then verify content of the SFT output file
