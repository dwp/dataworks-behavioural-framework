@hbase-export-flow
@fixture.k2hb.start
@fixture.k2hb.reconciler.start
@test
Feature: HBASE Snapshot Export Flow Test

  @fixture.hbase.clear.ingest.start
  @fixture.s3.clear.k2hb.manifests.main.start
  Scenario: We can snapshot hbase tables and export them to S3
    Given UCFS send '1' message of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'different'
        | input-file-name-kafka                                | output-file-name-kafka                             | snapshot-record-file-name-kafka |
        | current_valid_file_input.json                        | current_valid_file_output.json                     | None                            |
    And The latest timestamped 'kafka_main' message has been stored in HBase unaltered with id format of 'not wrapped'
    