@synthetic-data-ingestion
@fixture.historic.data.importer.start
Feature: Ingest synthetic data to HBase

    @fixture.hbase.clear.ingest.start
    Scenario: Transform synthetic raw data to the HDI input format
      Given synthetic raw data in datasets bucket
      When uploaded encrypted data and metadata files to ingest bucket
      And The import process is performed with skip existing records setting of 'false'

