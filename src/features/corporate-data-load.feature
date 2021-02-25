@data-load
@corporate-data-load
@fixture.k2hb.start
@test
Feature: UCFS Corporate Data Load to Dataworks

  Background: Tables have been created and relevant scripts downloaded to HBase
    Given The 'first_topic' topic override is used for this test
    And A bash 'download create tables script' step is started on the ingest-hbase EMR cluster
    And The 'download create tables script' step is executed successfully
    And The create tables process is started on HBase with arguments of 'test specific'
    And The create tables step is completed successfully
    And A bash 'download cdl script' step is started on the ingest-hbase EMR cluster
    And The 'download cdl script' step is executed successfully

  @fixture.s3.clear.corporate.data.start
  @fixture.hbase.clear.ingest.start
  Scenario: Files from S3 are loaded in to HBase with different not wrapped id input structures
    Given We generate corporate data of '10' records with the given template files with key method of 'different' and days offset of 'None'
        | input-file-name-corporate                            | output-file-name-corporate                         |
        | current_valid_file_input.json                        | current_valid_file_output.json                     |
        | missing_last_modified_date_input.json                | missing_last_modified_date_output.json             |
        | missing_last_modified_and_created_date_input.json    | missing_last_modified_and_created_date_output.json |
    When The corporate data is loaded in to HBase for the 'ucfs' metadata store with arguments of 'test specific', start date days offset of 'None', end date days offset of 'None', partition count of 'None' and prefix per execution setting of 'false'
    Then The data load is completed successfully with timeout setting of 'true'
    And The latest timestamped 'corporate' message has been stored in HBase unaltered with id format of 'not wrapped'

  @fixture.s3.clear.corporate.data.start
  @fixture.hbase.clear.ingest.start
  Scenario: Files from S3 are loaded in to HBase with start and end dates
    Given We generate corporate data of '10' records with the given template files with key method of 'different' and days offset of '-1'
        | input-file-name-corporate                            | output-file-name-corporate                         |
        | current_valid_file_input.json                        | current_valid_file_output.json                     |
    And We generate corporate data of '10' records with the given template files with key method of 'different' and days offset of '0'
        | input-file-name-corporate                            | output-file-name-corporate                         |
        | current_valid_file_input.json                        | current_valid_file_output.json                     |
    When The corporate data is loaded in to HBase for the 'ucfs' metadata store with arguments of 'test specific', start date days offset of '-1', end date days offset of '0', partition count of 'None' and prefix per execution setting of 'false'
    Then The data load is completed successfully with timeout setting of 'true'
    And The latest timestamped 'corporate' message has been stored in HBase unaltered with id format of 'not wrapped'

  @fixture.s3.clear.corporate.data.start
  @fixture.hbase.clear.ingest.start
  Scenario: Files from S3 are not loaded in to HBase due to being outside of desired date range to load
    Given We generate corporate data of '10' records with the given template files with key method of 'different' and days offset of '-3'
        | input-file-name-corporate                            | output-file-name-corporate                         |
        | current_valid_file_input.json                        | current_valid_file_output.json                     |
    When The corporate data is loaded in to HBase for the 'ucfs' metadata store with arguments of 'test specific', start date days offset of '-2', end date days offset of '-1', partition count of '3' and prefix per execution setting of 'true'
    Then The data load is completed successfully with timeout setting of 'long'
    And The latest timestamped 'corporate' message has not been stored in HBase unaltered with id format of 'not wrapped'
