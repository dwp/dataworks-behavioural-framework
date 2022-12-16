@corporate-data-ingestion
@test
@fixture.start.corporate_data_ingestion.cluster
@fixture.terminate.corporate_data_ingestion.cluster
Feature: Corporate data ingestion end to end test

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster ingests, decrypts and inserts valid records into Hive Tables
        Given the s3 source prefix is set to k2hb landing place in corporate bucket
        And The s3 'source' prefix is cleared
        And UCFS send '2' messages of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka          | output-file-name-kafka         | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json  | current_valid_file_output.json | None                             |
        When a step 'ingest-valid-records' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'COMPLETED'
        And confirm that '2' messages have been ingested
        When Hive table dumped into S3
        Then '2' records are available in exported data from the hive table

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster fails to process corrupted records
        Given the s3 source prefix is set to k2hb landing place in corporate bucket
        And The s3 'source' prefix is cleared
        And we generate a corrupted archive and store it in the Corporate Storage S3 bucket
        When a step 'ingest-corrupted-record' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster fails to read directory without permissions
        Given the s3 'source' prefix replaced by unauthorised location
        And UCFS send '2' messages of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka          | output-file-name-kafka         | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json  | current_valid_file_output.json | None                             |
        When a step 'no-read-permission-to-source' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster fails to write files to directory without permissions
        Given the s3 'destination' prefix replaced by unauthorised location
        When a step 'no-write-permission-to-destination' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster fails to process a record without dbObject key
        Given the s3 source prefix is set to k2hb landing place in corporate bucket
        And the s3 'source' prefix is cleared
        And UCFS send '1' message of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka          | output-file-name-kafka         | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json  | current_valid_file_output.json | None                             |
        When remove key 'dbObject' from existing file in s3 source prefix
        And a step 'ingest-record-without-dbObject' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster ingests, decrypts and insert records with empty bbObjects into Hive tables
        Given the s3 source prefix is set to k2hb landing place in corporate bucket
        And the s3 'source' prefix is cleared
        And UCFS send '1' message of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka          | output-file-name-kafka         | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json  | current_valid_file_output.json | None                             |
        When the value of 'dbObject' is replaced with 'None' from existing file in s3 source prefix
        And a step 'ingest-record-with-empty-dbObject' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'COMPLETED'
        When Hive table dumped into S3
        Then '1' records are available in exported data from the hive table

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster fails to ingest records without encryptedEncryptionKey
        Given the s3 source prefix is set to k2hb landing place in corporate bucket
        And the s3 'source' prefix is cleared
        And UCFS send '1' message of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka          | output-file-name-kafka         | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json  | current_valid_file_output.json | None                             |
        When remove key 'encryptedEncryptionKey' from existing file in s3 source prefix
        And a step 'ingest-record-without-encryptedEncryptionKey' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster fails to ingest records with an empty encryptedEncryptionKey value
        Given the s3 source prefix is set to k2hb landing place in corporate bucket
        And the s3 'source' prefix is cleared
        And UCFS send '1' message of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka          | output-file-name-kafka         | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json  | current_valid_file_output.json | None                             |
        When the value of 'encryptedEncryptionKey' is replaced with 'None' from existing file in s3 source prefix
        And a step 'ingest-record-with-empty-encryptedEncryptionKey' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster fails to ingest records with incorrect encryptedEncryptionKey value
        Given the s3 source prefix is set to k2hb landing place in corporate bucket
        And the s3 'source' prefix is cleared
        And UCFS send '1' message of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka          | output-file-name-kafka         | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json  | current_valid_file_output.json | None                             |
        When the value of 'encryptedEncryptionKey' is replaced with 'foobar' from existing file in s3 source prefix
        And a step 'ingest-record-with-incorrect-encryptedEncryptionKey' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster fails to ingest a malformed JSON record
        Given the s3 source prefix is set to k2hb landing place in corporate bucket
        And the s3 'source' prefix is cleared
        And UCFS send '1' message of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka          | output-file-name-kafka         | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json  | current_valid_file_output.json | None                             |
        When invalidate JSON from existing file in s3 source prefix
        And a step 'ingest-malformed-json-record' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster fails to ingest records without _lastModifiedDateTime key
        Given the s3 source prefix is set to k2hb landing place in corporate bucket
        And the s3 'source' prefix is cleared
        And UCFS send '1' message of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka          | output-file-name-kafka         | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json  | current_valid_file_output.json | None                             |
        When remove key '_lastModifiedDateTime' from existing file in s3 source prefix
        And a step 'ingest-record-without-lastModifiedDateTime' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'COMPLETED'
        And confirm that '1' messages have been ingested
        When Hive table dumped into S3
        Then '1' records are available in exported data from the hive table

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster fails to ingest records with empty _lastModifiedDateTime value
        Given the s3 source prefix is set to k2hb landing place in corporate bucket
        And the s3 'source' prefix is cleared
        And UCFS send '1' message of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka          | output-file-name-kafka         | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json  | current_valid_file_output.json | None                             |
        When the value of '_lastModifiedDateTime' is replaced with 'None' from existing file in s3 source prefix
        And a step 'ingest-record-with-empty-lastModifiedDateTime' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'COMPLETED'
        And confirm that '1' messages have been ingested
        When Hive table dumped into S3
        Then '1' records are available in exported data from the hive table

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Cluster fails to ingest records with incorrect _lastModifiedDateTime value
        Given the s3 source prefix is set to k2hb landing place in corporate bucket
        And the s3 'source' prefix is cleared
        And UCFS send '1' message of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka          | output-file-name-kafka         | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json  | current_valid_file_output.json | None                             |
        When the value of '_lastModifiedDateTime' is replaced with 'foobar' from existing file in s3 source prefix
        And a step 'ingest-record-with-incorrect-lastModifiedDateTime' is triggered on the EMR cluster corporate-data-ingestion
        Then confirm that the EMR step status is 'FAILED'
