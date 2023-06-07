@corporate-data-ingestion
@test
@fixture.start.corporate_data_ingestion.cluster
@fixture.terminate.corporate_data_ingestion.cluster
Feature: Corporate data ingestion end to end test

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster ingests, decrypts and inserts valid records into Hive Tables
        Given The 'kafka_audit' topic override is used for this test
        And The s3 'source' prefix is cleared
        And UCFS send '2' messages of type 'kafka_audit' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka                       | output-file-name-kafka                     | snapshot-record-file-name-kafka  |
            | current_valid_file_input_with_context.json  | current_valid_file_input_with_context.json | None                             |
        When a step 'ingest-valid-records' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'COMPLETED'
        When Hive table dumped into S3
        Then '2' records are available in exported data from the hive table

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster fails to process corrupted records
        Given The s3 'source' prefix is cleared
        And we generate a corrupted archive and store it in the Corporate Storage S3 bucket
        When a step 'ingest-corrupted-record' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster fails to read directory without permissions
        Given The s3 'source' prefix is cleared
        And The 'kafka_audit' topic override is used for this test
        And the s3 'source' prefix replaced by unauthorised location
        When a step 'no-read-permission-to-source' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'
        Then The s3 'source' prefix is cleared

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster fails to write files to directory without permissions
        Given the s3 'destination' prefix replaced by unauthorised location
        When a step 'no-write-permission-to-destination' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster fails to process a record without dbObject key
        Given The 'kafka_audit' topic override is used for this test
        And the s3 'source' prefix is cleared
        And UCFS send '1' messages of type 'kafka_audit' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka                       | output-file-name-kafka                     | snapshot-record-file-name-kafka  |
            | current_valid_file_input_with_context.json  | current_valid_file_input_with_context.json | None                             |
        When Key 'dbObject' is removed from existing file in s3 source prefix
        And a step 'ingest-record-without-dbObject' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster fails to ingest records without encryptedEncryptionKey
        Given the 'kafka_audit' topic override is used for this test
        And the s3 'source' prefix is cleared
        And UCFS send '1' messages of type 'kafka_audit' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka                       | output-file-name-kafka                     | snapshot-record-file-name-kafka  |
            | current_valid_file_input_with_context.json  | current_valid_file_input_with_context.json | None                             |
        When Key 'encryptedEncryptionKey' is removed from existing file in s3 source prefix
        And a step 'ingest-record-without-encryptedEncryptionKey' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster fails to ingest records with an empty encryptedEncryptionKey value
        Given the 'kafka_audit' topic override is used for this test
        And the s3 'source' prefix is cleared
        And UCFS send '1' messages of type 'kafka_audit' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka                       | output-file-name-kafka                     | snapshot-record-file-name-kafka  |
            | current_valid_file_input_with_context.json  | current_valid_file_input_with_context.json | None                             |
        When the value of 'encryptedEncryptionKey' is replaced with 'None' from existing file in s3 source prefix
        And a step 'ingest-record-with-empty-encryptedEncryptionKey' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster fails to ingest records with incorrect encryptedEncryptionKey value
        Given the 'kafka_audit' topic override is used for this test
        And the s3 'source' prefix is cleared
        And UCFS send '1' messages of type 'kafka_audit' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka                       | output-file-name-kafka                     | snapshot-record-file-name-kafka  |
            | current_valid_file_input_with_context.json  | current_valid_file_input_with_context.json | None                             |
        When the value of 'encryptedEncryptionKey' is replaced with 'foobar' from existing file in s3 source prefix
        And a step 'ingest-record-with-incorrect-encryptedEncryptionKey' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster fails to ingest a malformed JSON record
        Given the 'kafka_audit' topic override is used for this test
        And the s3 'source' prefix is cleared
        And UCFS send '1' messages of type 'kafka_audit' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka                       | output-file-name-kafka                     | snapshot-record-file-name-kafka  |
            | current_valid_file_input_with_context.json  | current_valid_file_input_with_context.json | None                             |
        When invalidate JSON from existing file in s3 source prefix
        And a step 'ingest-malformed-json-record' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster ingests records without _lastModifiedDateTime key
        Given the 'kafka_audit' topic override is used for this test
        And the s3 'source' prefix is cleared
        And UCFS send '1' messages of type 'kafka_audit' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka                       | output-file-name-kafka                     | snapshot-record-file-name-kafka  |
            | current_valid_file_input_with_context.json  | current_valid_file_input_with_context.json | None                             |
        When Key '_lastModifiedDateTime' is removed from existing file in s3 source prefix
        And a step 'ingest-record-without-lastModifiedDateTime' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'COMPLETED'
        When Hive table dumped into S3
        Then '1' records are available in exported data from the hive table

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster ingests records with empty _lastModifiedDateTime value
        Given the 'kafka_audit' topic override is used for this test
        And the s3 'source' prefix is cleared
        And UCFS send '1' messages of type 'kafka_audit' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka                       | output-file-name-kafka                     | snapshot-record-file-name-kafka  |
            | current_valid_file_input_with_context.json  | current_valid_file_input_with_context.json | None                             |
        When the value of '_lastModifiedDateTime' is replaced with 'None' from existing file in s3 source prefix
        And a step 'ingest-record-with-empty-lastModifiedDateTime' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'COMPLETED'
        When Hive table dumped into S3
        Then '1' records are available in exported data from the hive table
