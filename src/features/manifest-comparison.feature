Feature: Comparison of the manifests from import and export

  @manifest-comparison
  @fixture.s3.clear.manifest
  Scenario: The manifest comparison tables are populated
    Given The manifest generation tables have been created
    And The manifest generation tables have been populated

  @manifest-comparison-main
  Scenario: The manifest comparison report is generated and runs main queries
    When I generate the manifest comparison queries of type 'main'
    And I run the manifest comparison queries of type 'main' and upload the result to S3
    Then The query results match the expected results for queries of type 'main'

  @manifest-comparison-additional
  Scenario: The manifest comparison report is generated and runs additional queries
    When I generate the manifest comparison queries of type 'additional'
    And I run the manifest comparison queries of type 'additional' and upload the result to S3
    Then The query results match the expected results for queries of type 'additional'

  @manifest-comparison-specific
  Scenario: The manifest comparison report is generated and runs specific queries
    When I generate the manifest comparison queries of type 'specific'
    And I run the manifest comparison queries of type 'specific' and upload the result to S3
    Then The query results are printed
