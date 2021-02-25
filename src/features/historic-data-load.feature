@data-load
@historic-data-load
@fixture.k2hb.start
@test
Feature: UCFS Historic Data Load to Dataworks

  Background: Table creation has kicked off
    Given The 'first_topic' topic override is used for this test
    And A bash 'download create tables script' step is started on the ingest-hbase EMR cluster
    And The 'download create tables script' step is executed successfully
    And The create tables process is started on HBase with arguments of 'test specific'
    And The create tables step is completed successfully
    And A bash 'download hdl script' step is started on the ingest-hbase EMR cluster
    And The 'download hdl script' step is executed successfully

  @fixture.s3.clear.historic.data.start
  @fixture.hbase.clear.ingest.start
  Scenario: Files from S3 are loaded in to HBase with different wrapped id input structures
    Given UCFS upload '2' files for each of the given template files with '2' records and key method of 'different' and type of 'output'
        | input-file-name-import                                        | output-file-name-import                                       | snapshot-record-file-name-import    |
        | input_template_id_oid.json                                    | output_template_id_oid.json                                   | None                                |
        | input_template_id_string.json                                 | output_template_id_string.json                                | None                                |
    When The historic data is loaded in to HBase with arguments of 'test specific'
    Then The data load is completed successfully with timeout setting of 'true'
    And The relevant formatted data is stored in HBase with id format of 'wrapped'
  
  @fixture.s3.clear.historic.data.start
  @fixture.hbase.clear.ingest.start
  Scenario: Files from S3 are loaded in to HBase with different not wrapped id input structures
    Given UCFS upload '2' files for each of the given template files with '2' records and key method of 'different' and type of 'output'
            | input-file-name-import                                        | output-file-name-import                                       | snapshot-record-file-name-import    |
            | input_template.json                                           | output_template.json                                          | None                                |
            | input_template_removed.json                                   | output_template_removed.json                                  | None                                |
            | input_template_archived.json                                  | output_template_archived.json                                 | None                                |
            | input_template_id_with_embedded_date.json                     | output_template_id_with_embedded_date.json                    | None                                |
            | input_template_date_string.json                               | output_template_id_string.json                                | None                                |
            | input_template_missing_last_modified_date.json                | output_template_missing_last_modified_date.json               | None                                |
            | input_template_missing_last_modified_and_created_date.json    | output_template_missing_last_modified_and_created_date.json   | None                                |
    And UCFS upload '3' files for each of the given template files with '3' records and key method of 'single' and type of 'output'
            | input-file-name-import   | output-file-name-import    | snapshot-record-file-name-import    |
            | input_template.json      | output_template.json       | None                                |
    And UCFS upload '3' files for each of the given template files with '3' records and key method of 'file' and type of 'output'
            | input-file-name-import   | output-file-name-import    | snapshot-record-file-name-import    |
            | input_template.json      | output_template.json       | None                                |
    And UCFS upload '3' files for each of the given template files with '3' records and key method of 'record' and type of 'output'
            | input-file-name-import   | output-file-name-import    | snapshot-record-file-name-import    |
            | input_template.json      | output_template.json       | None                                |
    When The historic data is loaded in to HBase with arguments of 'test specific'
    Then The data load is completed successfully with timeout setting of 'true'
    And The relevant formatted data is stored in HBase with id format of 'not_wrapped'
