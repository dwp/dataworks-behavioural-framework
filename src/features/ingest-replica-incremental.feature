@ingest-replica
Feature: End to end test of ingest-replica data processing
    @fixture.hbase.clear.ingest.start
    @fixture.terminate.ingest_replica.cluster
    Scenario: Data added to HBase is processed by the ingest-replica cluster
        Given The incremental ingest-replica cluster is launched with no steps
        And UCFS send '30' messages of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka          | output-file-name-kafka         | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json  | current_valid_file_output.json | snapshot_record_valid.json       |
        And The latest timestamped 'kafka_main' message has been stored in HBase unaltered with id format of 'unwrapped'
        And Data added to hbase is flushed to S3
        And The S3 directory is cleared
        When The pyspark step is added to the ingest-replica cluster
        And Hive verification step is added to the cluster
        Then The processed data, '30' records, is available in HIVE
