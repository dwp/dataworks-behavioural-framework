@fixture.htme.start.full
@fixture.historic.data.importer.start
@test
Feature: UCFS Historic Data Ingestion to Dataworks with high load

  @load-test-full
  @fixture.hbase.clear.ingest.start
  @fixture.s3.clear.historic.data.start
  @fixture.s3.clear.snapshot.start
  Scenario: High load is imported in to HBase with one key per record in each file and exported to Snapshots
    When UCFS upload desired number of files with desired number of records per collection using load test key method
    And The import process is performed and a manifest is generated with skip existing records setting of 'false'
    Then The relevant formatted data is stored in HBase with id format of 'not_wrapped'
    And The export process is performed for snapshot type of 'full'
    And The number of snapshots created for each topic is '1' with match type of 'at least' and snapshot type of 'full'

  @load-test-upload
  Scenario: High load is uploaded to importer input location
    When UCFS upload desired number of input only files with desired number of records per collection using load test key method

  @load-test-import
  Scenario: Import process kicked off and manifest generated
    Given The import process is performed and a manifest is generated with skip existing records setting of 'false'

  @load-test-export
  Scenario: Exporter process kicked off and manifest generated
    Given The export process is performed for snapshot type of 'full'

  @load-test-clear-hdi
  @fixture.s3.clear.historic.data.start
  Scenario: S3 HDI files are cleared
    Given The S3 HDI files are cleared

  @load-test-clear-hbase
  @fixture.hbase.clear.ingest.start
  Scenario: S3 files are cleared
    Given HBase is cleared

  @load-test-clear-snapshots
  @fixture.s3.clear.snapshot.start
  Scenario: S3 snapshots are cleared
    Given The S3 snapshots are cleared

  @load-test-clear-all
  @fixture.hbase.clear.ingest.start
  @fixture.s3.clear.historic.data.start
  @fixture.s3.clear.snapshot.start
  Scenario: All files are cleared
    Given The S3 HDI files are cleared
    And HBase is cleared
    And The S3 snapshots are cleared

  @load-test-kafka
  Scenario Outline: A high number of kafka messages created
    Given UCFS send a high message volume of type 'kafka_main' with input file of '<input-file-name>' and output file of '<output-file-name>'
    Examples: Message types
        | input-file-name                                    | output-file-name                                   |
        | current_valid_file_input.json                      | current_valid_file_output.json                     |
