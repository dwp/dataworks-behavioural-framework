@hbase-export-flow
@fixture.k2hb.start
@fixture.k2hb.reconciler.start
@test
Feature: HBASE Snapshot Export Flow Test

  @fixture.hbase.clear.ingest.start
  @fixture.s3.clear.k2hb.manifests.main.start
  Scenario: We can snapshot hbase tables and export them to S3
    Given UCFS send '1' messages of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'different'
        | input-file-name-kafka                                | output-file-name-kafka                             | snapshot-record-file-name-kafka |
        | current_valid_file_input.json                        | current_valid_file_output.json                     | None                            |
    And The latest timestamped 'kafka_main' message has been stored in HBase unaltered with id format of 'not wrapped'
    And The checksums are uploaded
    When The HBASE Snapshot Export Flow is run
    And The HBASE Snapshot Export Flow is executed successfully
    Then The Snapshot is available in the S3 bucket