@data-egress
@fixture.k2hb.start
@fixture.htme.start.full
@fixture.snapshot.sender.start.max
@fixture.historic.data.importer.start
@test
Feature: Sending of snapshot data down to the existing analytical environment

    @fixture.s3.clear.snapshot
    @fixture.s3.clear.full.snapshot.output
    @fixture.s3.clear.historic.data.start
    @fixture.hbase.clear.ingest.start
    @fixture.dynamodb.clear.ingest.start.full
    Scenario: Files are sent in the snapshots with multiple different data scenarios
        Given UCFS send '2' messages of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka                                | output-file-name-kafka                             | snapshot-record-file-name-kafka                                   |
            | current_valid_file_input.json                        | current_valid_file_output.json                     | snapshot_record_valid.json                                        |
            | missing_last_modified_date_input.json                | missing_last_modified_date_output.json             | snapshot_record_valid_missing_last_modified_date.json             |
            | missing_last_modified_and_created_date_input.json    | missing_last_modified_and_created_date_output.json | snapshot_record_valid_missing_last_modified_and_created_date.json |
        And The latest timestamped 'kafka_main' message has been stored in HBase unaltered with id format of 'unwrapped'
        And UCFS upload '2' files for each of the given template files with '2' records and key method of 'different' and type of 'output'
            | input-file-name-import                                        | output-file-name-import                                       | snapshot-record-file-name-import                                  |
            | input_template_removed.json                                   | output_template_removed.json                                  | snapshot_record_valid_removed.json                                |
            | input_template_archived.json                                  | output_template_archived.json                                 | snapshot_record_valid_removed.json                                |
            | input_template_id_oid.json                                    | output_template_id_oid.json                                   | snapshot_record_valid_id_oid.json                                 |
            | input_template_id_string.json                                 | output_template_id_string.json                                | snapshot_record_valid_id_string.json                              |
            | input_template_id_with_embedded_date.json                     | output_template_id_with_embedded_date.json                    | snapshot_record_valid_id_with_embedded_date.json                  |
            | input_template_date_string.json                               | output_template_id_string.json                                | snapshot_record_valid_date_string.json                            |
            | input_template_missing_last_modified_date.json                | output_template_missing_last_modified_date.json               | snapshot_record_valid_missing_last_modified_date.json             |
            | input_template_missing_last_modified_and_created_date.json    | output_template_missing_last_modified_and_created_date.json   | snapshot_record_valid_missing_last_modified_and_created_date.json |
        When The import process is performed with skip existing records setting of 'false'
        And The relevant formatted data is stored in HBase with id format of 'not_wrapped'
        And The export and snapshot process is performed for snapshot type of 'full'
        And The dynamodb messages for each topic are one of 'Sent,Received,Success' for snapshot type of 'full'
        Then The number of snapshots created for each topic is '1' with match type of 'exact' and snapshot type of 'full'
        And Snapshot sender sends the correct snapshots for snapshot type of 'full'
        And The dynamodb messages for each topic are one of 'Success' for snapshot type of 'full'
        