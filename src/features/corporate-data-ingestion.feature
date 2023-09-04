@corporate-data-ingestion
@test
@fixture.k2hb.start
@fixture.start.corporate_data_ingestion.cluster
@fixture.terminate.corporate_data_ingestion.cluster
Feature: Corporate data ingestion end to end test
    Scenario: Cluster ingests calculationParts-like records and produces exports with latest versions
        Given The 'main' prefixes are used
        And The source and destination prefixes are cleared
        Given UCFS send '40' messages of type 'calculation_parts' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'static' with the same Ids
            | input-file-name-kafka                | output-file-name-kafka                 | snapshot-record-file-name-kafka  |
            | current_valid_file_input.json        | current_valid_file_output.json         | None                             |
            | current_valid_file_delete_input.json | current_valid_file_delete_output.json  | None                             |
        Given Empty orc snapshot placed in S3
        When a step 'ingest-valid-records' is triggered on the EMR cluster corporate-data-ingestion with 'calculator:calculationParts' ingestion class with additional parameters '--force_collection_update'
        Then confirm that the EMR step status is 'COMPLETED'
        When Daily Data is dumped to S3
        Then '40' records are available in exported data from the hive table
        When Export Data is dumped to S3
        Then '20' records are available in exported data from the hive table

    Scenario: Cluster ingests, decrypts and inserts valid records into Hive Tables
        Given The 'kafka_audit' topic override is used for this test
        And The 'audit' prefixes are used
        And The source and destination prefixes are cleared
        And UCFS send '2' messages of type 'kafka_audit' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka                       | output-file-name-kafka                     | snapshot-record-file-name-kafka  |
            | current_valid_file_input_with_context.json  | current_valid_file_input_with_context.json | None                             |
        When a step 'ingest-valid-records' is triggered on the EMR cluster corporate-data-ingestion with 'None' ingestion class with additional parameters 'None'
        Then confirm that the EMR step status is 'COMPLETED'
        When The audit hive table is dumped into S3
        Then '2' records are available in exported data from the hive table

    Scenario: Cluster fails to process corrupted records
        Given The 'kafka_audit' topic override is used for this test
        And The 'audit' prefixes are used
        And The source and destination prefixes are cleared
        And we generate a corrupted archive and store it in the Corporate Storage S3 bucket
        When a step 'ingest-corrupted-record' is triggered on the EMR cluster corporate-data-ingestion with 'None' ingestion class with additional parameters 'None'
        Then confirm that the EMR step status is 'FAILED'

    Scenario: Cluster fails to process a record without dbObject key
        Given The 'kafka_audit' topic override is used for this test
        And The 'audit' prefixes are used
        And The source and destination prefixes are cleared
        And UCFS send '2' messages of type 'kafka_audit' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka                       | output-file-name-kafka                     | snapshot-record-file-name-kafka  |
            | current_valid_file_input_with_context.json  | current_valid_file_input_with_context.json | None                             |
        When Key 'dbObject' is removed from existing file in s3 source prefix
        And a step 'ingest-record-without-dbObject' is triggered on the EMR cluster corporate-data-ingestion with 'None' ingestion class with additional parameters 'None'
        Then confirm that the EMR step status is 'FAILED'

    Scenario: Cluster fails to ingest records without encryptedEncryptionKey
        Given The 'kafka_audit' topic override is used for this test
        And The 'audit' prefixes are used
        And The source and destination prefixes are cleared
        And UCFS send '2' messages of type 'kafka_audit' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka                       | output-file-name-kafka                     | snapshot-record-file-name-kafka  |
            | current_valid_file_input_with_context.json  | current_valid_file_input_with_context.json | None                             |
        When Key 'encryptedEncryptionKey' is removed from existing file in s3 source prefix
        And a step 'ingest-record-without-encryptedEncryptionKey' is triggered on the EMR cluster corporate-data-ingestion with 'None' ingestion class with additional parameters 'None'
        Then confirm that the EMR step status is 'FAILED'

    Scenario: Cluster fails to ingest records with an empty encryptedEncryptionKey value
        Given The 'kafka_audit' topic override is used for this test
        And The 'audit' prefixes are used
        And The source and destination prefixes are cleared
        And UCFS send '2' messages of type 'kafka_audit' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka                       | output-file-name-kafka                     | snapshot-record-file-name-kafka  |
            | current_valid_file_input_with_context.json  | current_valid_file_input_with_context.json | None                             |
        When the value of 'encryptedEncryptionKey' is replaced with 'None' from existing file in s3 source prefix
        And a step 'ingest-record-with-empty-encryptedEncryptionKey' is triggered on the EMR cluster corporate-data-ingestion with 'None' ingestion class with additional parameters 'None'
        Then confirm that the EMR step status is 'FAILED'

    Scenario: Cluster fails to ingest records with incorrect encryptedEncryptionKey value
        Given The 'kafka_audit' topic override is used for this test
        And The 'audit' prefixes are used
        And The source and destination prefixes are cleared
        And UCFS send '2' messages of type 'kafka_audit' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka                       | output-file-name-kafka                     | snapshot-record-file-name-kafka  |
            | current_valid_file_input_with_context.json  | current_valid_file_input_with_context.json | None                             |
        When the value of 'encryptedEncryptionKey' is replaced with 'foobar' from existing file in s3 source prefix
        And a step 'ingest-record-with-incorrect-encryptedEncryptionKey' is triggered on the EMR cluster corporate-data-ingestion with 'None' ingestion class with additional parameters 'None'
        Then confirm that the EMR step status is 'FAILED'

    Scenario: Cluster fails to ingest a malformed JSON record
        Given The 'kafka_audit' topic override is used for this test
        And The 'audit' prefixes are used
        And The source and destination prefixes are cleared
        And UCFS send '2' messages of type 'kafka_audit' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka                       | output-file-name-kafka                     | snapshot-record-file-name-kafka  |
            | current_valid_file_input_with_context.json  | current_valid_file_input_with_context.json | None                             |
        When invalidate JSON from existing file in s3 source prefix
        And a step 'ingest-malformed-json-record' is triggered on the EMR cluster corporate-data-ingestion with 'None' ingestion class with additional parameters 'None'
        Then confirm that the EMR step status is 'FAILED'

    Scenario: Cluster ingests records without _lastModifiedDateTime key
        Given The 'kafka_audit' topic override is used for this test
        And The 'audit' prefixes are used
        And The source and destination prefixes are cleared
        And UCFS send '2' messages of type 'kafka_audit' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka                       | output-file-name-kafka                     | snapshot-record-file-name-kafka  |
            | current_valid_file_input_with_context.json  | current_valid_file_input_with_context.json | None                             |
        When Key '_lastModifiedDateTime' is removed from existing file in s3 source prefix
        And a step 'ingest-record-without-lastModifiedDateTime' is triggered on the EMR cluster corporate-data-ingestion with 'None' ingestion class with additional parameters 'None'
        Then confirm that the EMR step status is 'COMPLETED'
        When The audit hive table is dumped into S3
        Then '2' records are available in exported data from the hive table

    Scenario: Cluster ingests records with empty _lastModifiedDateTime value
        Given The 'kafka_audit' topic override is used for this test
        And The 'audit' prefixes are used
        And The source and destination prefixes are cleared
        And UCFS send '2' messages of type 'kafka_audit' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'message'
            | input-file-name-kafka                       | output-file-name-kafka                     | snapshot-record-file-name-kafka  |
            | current_valid_file_input_with_context.json  | current_valid_file_input_with_context.json | None                             |
        When the value of '_lastModifiedDateTime' is replaced with 'None' from existing file in s3 source prefix
        And a step 'ingest-record-with-empty-lastModifiedDateTime' is triggered on the EMR cluster corporate-data-ingestion with 'None' ingestion class with additional parameters 'None'
        Then confirm that the EMR step status is 'COMPLETED'
        When The audit hive table is dumped into S3
        Then '2' records are available in exported data from the hive table
