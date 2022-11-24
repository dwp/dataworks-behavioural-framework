@corporate-data-ingestion
@test
@fixture.start.corporate_data_ingestion.cluster
@fixture.terminate.corporate_data_ingestion.cluster
Feature: Corporate data ingestion end to end test

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Ingesting valid records
        Given s3 source prefix set to k2hb landing place in corporate bucket
        Given clean s3 'source' prefix
        Given UCFS send '2' messages of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka          | output-file-name-kafka         | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json  | current_valid_file_output.json | None                             |
        When a step 'ingest-valid-record' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'COMPLETED'
        Then confirm that '2' messages have been ingested

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Ingesting a corrupted record
        Given s3 source prefix set to k2hb landing place in corporate bucket
        Given clean s3 'source' prefix
        Given we generate a corrupted archive and store it in the Corporate Storage S3 bucket
        When a step 'ingest-corrupted-record' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'
#
    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Ingesting records without read access to s3 prefix
        Given s3 'source' prefix replaced by unauthorised location
        Given UCFS send '2' messages of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka          | output-file-name-kafka         | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json  | current_valid_file_output.json | None                             |
        When a step 'no-read-permission-to-source' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Ingesting records without write access to s3 prefix
        Given s3 'destination' prefix replaced by unauthorised location
        When a step 'no-write-permission-to-destination' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Ingesting a record without dbObject key
        Given s3 source prefix set to k2hb landing place in corporate bucket
        Given clean s3 'source' prefix
        Given UCFS send '1' message of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka          | output-file-name-kafka         | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json  | current_valid_file_output.json | None                             |
        When remove key 'dbObject' from existing file in s3 source prefix
        When a step 'ingest-record-without-dbObject' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Ingesting a record with an empty dbObject value
        Given s3 source prefix set to k2hb landing place in corporate bucket
        Given clean s3 'source' prefix
        Given UCFS send '1' message of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka          | output-file-name-kafka         | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json  | current_valid_file_output.json | None                             |
        When replace value of 'dbObject' by 'None' from existing file in s3 source prefix
        When a step 'ingest-record-with-empty-dbObject' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'COMPLETED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Ingesting a record without encryptedEncryptionKey key
        Given s3 source prefix set to k2hb landing place in corporate bucket
        Given clean s3 'source' prefix
        Given UCFS send '1' message of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka          | output-file-name-kafka         | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json  | current_valid_file_output.json | None                             |
        When remove key 'encryptedEncryptionKey' from existing file in s3 source prefix
        When a step 'ingest-record-without-encryptedEncryptionKey' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Ingesting a record with an empty encryptedEncryptionKey value
        Given s3 source prefix set to k2hb landing place in corporate bucket
        Given clean s3 'source' prefix
        Given UCFS send '1' message of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka          | output-file-name-kafka         | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json  | current_valid_file_output.json | None                             |
        When replace value of 'encryptedEncryptionKey' by 'None' from existing file in s3 source prefix
        When a step 'ingest-record-with-empty-encryptedEncryptionKey' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Ingesting a record with an incorrect encryptedEncryptionKey value
        Given s3 source prefix set to k2hb landing place in corporate bucket
        Given clean s3 'source' prefix
        Given UCFS send '1' message of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka          | output-file-name-kafka         | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json  | current_valid_file_output.json | None                             |
        When replace value of 'encryptedEncryptionKey' by 'foobar' from existing file in s3 source prefix
        When a step 'ingest-record-with-incorrect-encryptedEncryptionKey' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Ingesting a malformed JSON record
        Given s3 source prefix set to k2hb landing place in corporate bucket
        Given clean s3 'source' prefix
        Given UCFS send '1' message of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka          | output-file-name-kafka         | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json  | current_valid_file_output.json | None                             |
        When invalidate JSON from existing file in s3 source prefix
        When a step 'ingest-malformed-json-record' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'
