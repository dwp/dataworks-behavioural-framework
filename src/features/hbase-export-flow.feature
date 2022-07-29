@hbase-export-flow
@fixture.k2hb.start
@fixture.k2hb.reconciler.start
@test
Feature: HBASE Snapshot Export Flow Test

  @fixture.hbase.clear.ingest.start
  @fixture.s3.clear.k2hb.manifests.main.start
  @fixture.clean.up.hbase.export.s3.bucket
  @fixture.clean.up.hbase.export.hbase.snapshots
  Scenario: We can snapshot hbase tables and export them to S3
    Given UCFS send '1' messages of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'different'
        | input-file-name-kafka                                | output-file-name-kafka                             | snapshot-record-file-name-kafka |
        | current_valid_file_input.json                        | current_valid_file_output.json                     | None                            |
    And The latest timestamped 'kafka_main' message has been stored in HBase unaltered with id format of 'not wrapped'
    And The checksums are uploaded
    When The HBASE Snapshot Export script is downloaded on the ingest-hbase EMR cluster
    And The Download HBASE Export script step is executed successfully
    And The HBASE Snapshot Export script is run with HBASE snapshot name 'automated_tests_snapshot'
    And The HBASE Snapshot Export step is executed successfully
    Then The HBASE Snapshot 'automated_tests_snapshot' is available in the Export S3 bucket
