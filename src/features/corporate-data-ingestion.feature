@corporate-data-ingestion
@test
@fixture.start.corporate_data_ingestion.cluster
@fixture.terminate.corporate_data_ingestion.cluster
Feature: Corporate data ingestion end to end test

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Ingesting valid records
        Given we generate '5' records with the input template 'current_valid_file_input.json' and store them in the Corporate Storage S3 bucket
        When corporate-data-ingestion EMR step triggered
        Then confirm that the EMR step status is 'COMPLETED'
        Then confirm that the number of generated records equals the number of ingested records

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Ingesting a corrupted record amongst valid records
        Given we generate '5' records with the input template 'current_valid_file_input.json' and store them in the Corporate Storage S3 bucket
        Given we generate a corrupted archive and store it in the Corporate Storage S3 bucket
        When corporate-data-ingestion EMR step triggered
        Then confirm that the EMR step status is 'FAILED'
        Then confirm that the number of generated records equals the number of ingested records

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Ingesting records without read access to s3 prefix
        Given s3 'source' prefix replaced by unauthorised location
        Given we generate '5' records with the input template 'current_valid_file_input.json' and store them in the Corporate Storage S3 bucket
        When corporate-data-ingestion EMR step triggered
        Then confirm that the EMR step status is 'FAILED'

    @fixture.prepare.corporate.data.ingestion.context
    @fixture.s3.clear.corporate.data.ingestion.prefixes
    Scenario: Ingesting records without write access to s3 prefix
        Given s3 'destination' prefix replaced by unauthorised location
        Given we generate '5' records with the input template 'current_valid_file_input.json' and store them in the Corporate Storage S3 bucket
        When corporate-data-ingestion EMR step triggered
        Then confirm that the EMR step status is 'FAILED'
