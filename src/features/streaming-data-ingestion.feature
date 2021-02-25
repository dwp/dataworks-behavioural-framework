@data-streaming
@fixture.k2hb.start
@fixture.k2hb.reconciler.start
@test
Feature: UCFS Business Data Ingestion into DataWorks from Kafka

  @data-streaming-main
  @fixture.hbase.clear.ingest.start
  @fixture.s3.clear.k2hb.manifests.main.start
  Scenario: We can ingest messages from UC on the main schema with differing formats
    Given UCFS send '2' messages of type 'kafka_main' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'different'
        | input-file-name-kafka                                | output-file-name-kafka                             | snapshot-record-file-name-kafka |
        | current_valid_file_input.json                        | current_valid_file_output.json                     | None                            |
        | missing_last_modified_date_input.json                | missing_last_modified_date_output.json             | None                            |
        | missing_last_modified_and_created_date_input.json    | missing_last_modified_and_created_date_output.json | None                            |
    Then The latest timestamped 'kafka_main' message has been stored in HBase unaltered with id format of 'not wrapped'
    And The latest id and timestamp have been correctly written to the 'main' metadata table with id format of 'not_wrapped' for message type 'kafka_main'
    And The latest id and timestamp have been correctly logged to the streaming 'main' manifests with id format of 'not wrapped' for message type 'kafka_main'
    And The reconciler reconciles the latest id and timestamp to the 'main' metadata table

  @fixture.hbase.clear.ingest.equalities.start
  @fixture.s3.clear.k2hb.manifests.equalities.start
  Scenario: We can ingest messages from UC on the equalities schema with differing formats
    Given The 'kafka_equalities' topic override is used for this test
    And UCFS send '2' messages of type 'kafka_equalities' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'different'
        | input-file-name-kafka                                | output-file-name-kafka                             | snapshot-record-file-name-kafka |
        | current_valid_file_input.json                        | current_valid_file_output.json                     | None                            |
        | missing_last_modified_date_input.json                | missing_last_modified_date_output.json             | None                            |
        | missing_last_modified_and_created_date_input.json    | missing_last_modified_and_created_date_output.json | None                            |
    Then The latest timestamped 'kafka_equalities' message has been stored in HBase unaltered with id format of 'not wrapped'
    And The latest id and timestamp have been correctly written to the 'equalities' metadata table with id format of 'not_wrapped' for message type 'kafka_equalities'
    And The latest id and timestamp have been correctly logged to the streaming 'equalities' manifests with id format of 'not wrapped' for message type 'kafka_equalities'
    And The reconciler reconciles the latest id and timestamp to the 'equalities' metadata table

  @fixture.hbase.clear.ingest.audit.start
  @fixture.s3.clear.k2hb.manifests.audit.start
  Scenario: We can ingest messages from UC on the audit schema with differing formats
    Given The 'kafka_audit' topic override is used for this test
    And UCFS send '2' messages of type 'kafka_audit' with the given template files, encryption setting of 'true' and wait setting of 'true' with key method of 'different'
        | input-file-name-kafka                                | output-file-name-kafka                             | snapshot-record-file-name-kafka |
        | current_valid_file_input.json                        | current_valid_file_output.json                     | None                            |
        | missing_last_modified_date_input.json                | missing_last_modified_date_output.json             | None                            |
        | missing_last_modified_and_created_date_input.json    | missing_last_modified_and_created_date_output.json | None                            |
    Then The latest timestamped 'kafka_audit' message has been stored in HBase unaltered with id format of 'not wrapped'
    And The latest id and timestamp have been correctly written to the 'audit' metadata table with id format of 'not_wrapped' for message type 'kafka_audit'
    And The latest id and timestamp have been correctly logged to the streaming 'audit' manifests with id format of 'not wrapped' for message type 'kafka_audit'
    And The reconciler reconciles the latest id and timestamp to the 'audit' metadata table

  @data-streaming-main
  @fixture.s3.clear.dlq
  @fixture.hbase.clear.ingest.start
  Scenario Outline: UC send bad messages on the main schema so we put them in the DLQ
    Given UCFS send '1' message of type 'kafka_main' with input file of '<input-file-name>', output file of 'None', dlq file of '<dlq-file-name>', snapshot record file of 'None', encryption setting of '<encrypt-in-sender>' and wait setting of 'true' with key method of 'static'
    Then A single output message matching the Kafka dlq file '<dlq-file-name>' will be stored in the dlq S3 bucket folder
    And A message for the Kafka dlq file '<dlq-file-name>' will not be stored in HBase
    And The latest id from the Kafka dlq file '<dlq-file-name>' has not been written to the 'main' metadata table with id format of 'not wrapped'
    Examples: Bad messages
        | input-file-name              | dlq-file-name                 | encrypt-in-sender |
        | missing_collection_in.json   | missing_collection_dlq.json   | true              |
        | missing_database_in.json     | missing_database_dlq.json     | true              |
        | unencrypted_dbObject_in.json | unencrypted_dbObject_dlq.json | false             |
        | missing_dbObject_in.json     | missing_dbObject_dlq.json     | false             |
        | message_not_valid_in.json    | message_not_valid_dlq.json    | false             |

  @fixture.s3.clear.dlq
  @fixture.hbase.clear.ingest.equalities.start
  Scenario Outline: UC send bad messages on the equalities schema so we put them in the DLQ
    Given The 'kafka_equalities' topic override is used for this test
    And UCFS send '1' message of type 'kafka_equalities' with input file of '<input-file-name>', output file of 'None', dlq file of '<dlq-file-name>', snapshot record file of 'None', encryption setting of '<encrypt-in-sender>' and wait setting of 'true' with key method of 'static'
    Then A single output message matching the Kafka dlq file '<dlq-file-name>' will be stored in the dlq S3 bucket folder
    And A message for the Kafka dlq file '<dlq-file-name>' will not be stored in HBase
    And The latest id from the Kafka dlq file '<dlq-file-name>' has not been written to the 'equalities' metadata table with id format of 'not wrapped'
    Examples: Bad messages
        | input-file-name              | dlq-file-name                 | encrypt-in-sender |
        | unencrypted_dbObject_in.json | unencrypted_dbObject_dlq.json | false             |
        | missing_dbObject_in.json     | missing_dbObject_dlq.json     | false             |
        | message_not_valid_in.json    | message_not_valid_dlq.json    | false             |

  @fixture.s3.clear.dlq
  @fixture.hbase.clear.ingest.audit.start
  Scenario Outline: UC send bad messages on the audit schema so we put them in the DLQ
    Given The 'kafka_audit' topic override is used for this test
    And UCFS send '1' message of type 'kafka_audit' with input file of '<input-file-name>', output file of 'None', dlq file of '<dlq-file-name>', snapshot record file of 'None', encryption setting of '<encrypt-in-sender>' and wait setting of 'true' with key method of 'static'
    Then A single output message matching the Kafka dlq file '<dlq-file-name>' will be stored in the dlq S3 bucket folder
    And A message for the Kafka dlq file '<dlq-file-name>' will not be stored in HBase
    And The latest id from the Kafka dlq file '<dlq-file-name>' has not been written to the 'audit' metadata table with id format of 'not wrapped'
    Examples: Bad messages
        | input-file-name              | dlq-file-name                 | encrypt-in-sender |
        | unencrypted_dbObject_in.json | unencrypted_dbObject_dlq.json | false             |
        | missing_dbObject_in.json     | missing_dbObject_dlq.json     | false             |
        | message_not_valid_in.json    | message_not_valid_dlq.json    | false             |
