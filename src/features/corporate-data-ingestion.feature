@corporate-data-ingestion
@test
@fixture.start.corporate_data_ingestion.cluster
@fixture.terminate.corporate_data_ingestion.cluster
Feature: Corporate data ingestion end to end test

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster ingests, decrypts and inserts valid records into Hive Tables
        Given s3 source prefix set to k2hb landing place in corporate bucket
        Given clean s3 'source' prefix
        Given UCFS send '2' messages of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka          | output-file-name-kafka         | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json  | current_valid_file_output.json | None                             |
        When a step 'ingest-valid-records' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'COMPLETED'
        Then confirm that '2' messages have been ingested
        When Hive table dumped into S3
        Then '2' records are available in exported data from the hive table

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster fails to process corrupted records
        Given s3 source prefix set to k2hb landing place in corporate bucket
        Given clean s3 'source' prefix
        Given we generate a corrupted archive and store it in the Corporate Storage S3 bucket
        When a step 'ingest-corrupted-record' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster fails to read directory without permissions
        Given s3 'source' prefix replaced by unauthorised location
        Given UCFS send '2' messages of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka          | output-file-name-kafka         | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json  | current_valid_file_output.json | None                             |
        When a step 'no-read-permission-to-source' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster fails to write files to directory without permissions
        Given s3 'destination' prefix replaced by unauthorised location
        When a step 'no-write-permission-to-destination' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster fails to process a record without dbObject key
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
    Scenario: Cluster ingests, decrypts and insert records with empty bbObjects into Hive tables
        Given s3 source prefix set to k2hb landing place in corporate bucket
        Given clean s3 'source' prefix
        Given UCFS send '1' message of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka          | output-file-name-kafka         | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json  | current_valid_file_output.json | None                             |
        When replace value of 'dbObject' by 'None' from existing file in s3 source prefix
        When a step 'ingest-record-with-empty-dbObject' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'COMPLETED'
        When Hive table dumped into S3
        Then '1' records are available in exported data from the hive table

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster fails to ingest records without encryptedEncryptionKey
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
    Scenario: Cluster fails to ingest records with an empty encryptedEncryptionKey value
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
    Scenario: Cluster fails to ingest records with incorrect encryptedEncryptionKey value
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
    Scenario: Cluster fails to ingest a malformed JSON record
        Given s3 source prefix set to k2hb landing place in corporate bucket
        Given clean s3 'source' prefix
        Given UCFS send '1' message of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka          | output-file-name-kafka         | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json  | current_valid_file_output.json | None                             |
        When invalidate JSON from existing file in s3 source prefix
        When a step 'ingest-malformed-json-record' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster fails to ingest records without _lastModifiedDateTime key
        Given s3 source prefix set to k2hb landing place in corporate bucket
        Given clean s3 'source' prefix
        Given UCFS send '1' message of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka          | output-file-name-kafka         | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json  | current_valid_file_output.json | None                             |
        When remove key '_lastModifiedDateTime' from existing file in s3 source prefix
        When a step 'ingest-record-without-lastModifiedDateTime' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster fails to ingest records with empty _lastModifiedDateTime value
        Given s3 source prefix set to k2hb landing place in corporate bucket
        Given clean s3 'source' prefix
        Given UCFS send '1' message of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka          | output-file-name-kafka         | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json  | current_valid_file_output.json | None                             |
        When replace value of '_lastModifiedDateTime' by 'None' from existing file in s3 source prefix
        When a step 'ingest-record-with-empty-lastModifiedDateTime' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster fails to ingest records with incorrect _lastModifiedDateTime value
        Given s3 source prefix set to k2hb landing place in corporate bucket
        Given clean s3 'source' prefix
        Given UCFS send '1' message of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka          | output-file-name-kafka         | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json  | current_valid_file_output.json | None                             |
        When replace value of '_lastModifiedDateTime' by 'foobar' from existing file in s3 source prefix
        When a step 'ingest-record-with-incorrect-lastModifiedDateTime' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'
