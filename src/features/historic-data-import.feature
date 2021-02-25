@data-import
@fixture.historic.data.importer.start
@fixture.k2hb.start
@test
Feature: UCFS Historic Data Ingestion to Dataworks

  @fixture.s3.clear.historic.data.start
  @fixture.hbase.clear.ingest.start
  Scenario: Files from S3 are imported in to HBase with different wrapped id input structures
    Given UCFS upload '2' files for each of the given template files with '2' records and key method of 'different' and type of 'output'
        | input-file-name-import                                        | output-file-name-import                                       | snapshot-record-file-name-import    |
        | input_template_id_oid.json                                    | output_template_id_oid.json                                   | None                                |
        | input_template_id_string.json                                 | output_template_id_string.json                                | None                                |
    When The import process is performed with skip existing records setting of 'true'
    And The relevant formatted data is stored in HBase with id format of 'wrapped'
  
  @fixture.s3.clear.historic.data.start
  @fixture.hbase.clear.ingest.start
  Scenario: Files from S3 are imported in to HBase with different not wrapped id input structures
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
    When The import process is performed with skip existing records setting of 'false'
    And The relevant formatted data is stored in HBase with id format of 'not_wrapped'

  @fixture.s3.clear.historic.data.start
  @fixture.hbase.clear.ingest.start
  Scenario: Files from S3 are imported in to HBase while ignoring skipped records but no records are uploaded that should be skipped
    Given UCFS upload '2' files for each of the given template files with '2' records and key method of 'different' and type of 'output'
        | input-file-name-import                    | output-file-name-import                       | snapshot-record-file-name-import    |
        | input_template_christmas_day_2017.json    | output_template_christmas_day_2017.json       | None                                |
        | input_template_christmas_day_2018.json    | output_template_christmas_day_2018.json       | None                                |
    And The skip earlier time override is set as '2017-12-24T01:02:03.111'
    And The skip later time override is set as '2018-12-25T12:23:26.000'
    When The import process is performed with skip existing records setting of 'false'
    And The relevant formatted data is stored in HBase with id format of 'not_wrapped'

  @fixture.s3.clear.historic.data.start
  @fixture.hbase.clear.ingest.start
  Scenario: Files from S3 are imported in to HBase while ignoring skipped records and multiple records are uploaded that should be skipped
    Given UCFS upload '2' files for each of the given template files with '2' records and key method of 'different' and type of 'output'
        | input-file-name-import                    | output-file-name-import                   | snapshot-record-file-name-import    |
        | input_template_christmas_day_2016.json    | output_template_christmas_day_2016.json   | None                                |
        | input_template_christmas_day_2019.json    | output_template_christmas_day_2019.json   | None                                |
    And The skip earlier time override is set as '2017-12-24T01:02:03.111'
    And The skip later time override is set as '2018-12-25T12:23:26.000'
    When The import process is performed with skip existing records setting of 'false'
    And The relevant formatted data is not stored in HBase with id format of 'not_wrapped'
