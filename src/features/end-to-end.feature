@end-to-end
@fixture.historic.data.importer.start
@fixture.k2hb.start
@fixture.htme.start.full
@test
Feature: UCFS Business Data Ingestion full end-to-end in to Crown

    @pull-request
    @fixture.snapshot.sender.start.max
    @fixture.s3.clear.snapshot
    @fixture.s3.clear.full.snapshot.output
    @fixture.s3.clear.historic.data.start
    @fixture.hbase.clear.ingest.start
    @fixture.dynamodb.clear.ingest.start.full
    Scenario: Data takes the full end to end journey through the system
        Given UCFS send '2' messages of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka          | output-file-name-kafka         | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json  | current_valid_file_output.json | snapshot_record_valid.json       |
        And The latest timestamped 'kafka_main' message has been stored in HBase unaltered with id format of 'unwrapped'
        And UCFS upload '2' files for each of the given template files with '2' records and key method of 'different' and type of 'output'
            | input-file-name-import  | output-file-name-import | snapshot-record-file-name-import |
            | input_template.json     | output_template.json    | snapshot_record_valid.json       |
        When The import process is performed with skip existing records setting of 'false'
        And The relevant formatted data is stored in HBase with id format of 'not_wrapped'
        And The export and snapshot process is performed for snapshot type of 'full'
        And The dynamodb messages for each topic are one of 'Sent,Received,Success' for snapshot type of 'full'
        And The dynamodb status for 'HTME' is set to 'COMPLETED' with snapshot type of 'full'
        Then The number of snapshots created for each topic is '1' with match type of 'exact' and snapshot type of 'full'
        And Snapshot sender sends the correct snapshots for snapshot type of 'full'
        And The dynamodb messages for each topic are one of 'Success' for snapshot type of 'full'
        And The dynamodb status for 'SNAPSHOT_SENDER' is set to 'COMPLETED' with snapshot type of 'full'

    @pull-request
    @fixture.s3.clear.historic.data.start
    @fixture.hbase.clear.ingest.start
    Scenario: We can ingest a version of a record later than an already imported version
    Given UCFS upload '1' files for each of the given template files with '1' records and key method of 'static' and type of 'output'
        | input-file-name-import      | output-file-name-import      | snapshot-record-file-name-import |
        | input_template.json         | output_template.json         | None                             |
    When The import process is performed with skip existing records setting of 'false'
    And The relevant formatted data is stored in HBase with id format of 'not_wrapped'
    And UCFS send the same message of type 'kafka_main' via Kafka with a later timestamp
    Then The latest timestamped 'kafka_main' message has been stored in HBase unaltered with id format of 'unwrapped'
