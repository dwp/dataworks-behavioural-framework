Feature: Processes to load and import data in to ingest hbase

  @admin-historic-data-import
  @fixture.historic.data.importer.start.data.load
  Scenario: Start an import of snapshots from the S3 input bucket to HBase
    Given The UC data import prefixes are configured
    Then The UC historic data is imported from each prefix with setting of 'import and manifest' and skip existing records setting of 'false'

  @admin-historic-data-import-manifest-only
  @fixture.historic.data.importer.start.data.load
  Scenario: Start an import of snapshots from the S3 input bucket to HBase
    Given The UC data import prefixes are configured
    Then The UC historic data is imported from each prefix with setting of 'manifest' and skip existing records setting of 'false'

  @admin-historic-data-import-import-only
  @fixture.historic.data.importer.start.data.load
  Scenario: Start an import of snapshots from the S3 input bucket to HBase
    Given The UC data import prefixes are configured
    Then The UC historic data is imported from each prefix with setting of 'import' and skip existing records setting of 'false'

  @admin-historic-data-import-with-skip
  @fixture.historic.data.importer.start.data.load
  Scenario: Start an import of snapshots from the S3 input bucket to HBase while skipping existing records
    Given The UC data import prefixes are configured
    Then The UC historic data is imported from each prefix with setting of 'import and manifest' and skip existing records setting of 'true'

  @admin-historic-data-import-with-skip-manifest-only
  @fixture.historic.data.importer.start.data.load
  Scenario: Start an import of snapshots from the S3 input bucket to HBase while skipping existing records
    Given The UC data import prefixes are configured
    Then The UC historic data is imported from each prefix with setting of 'manifest' and skip existing records setting of 'true'

  @admin-historic-data-import-with-skip-import-only
  @fixture.historic.data.importer.start.data.load
  Scenario: Start an import of snapshots from the S3 input bucket to HBase while skipping existing records
    Given The UC data import prefixes are configured
    Then The UC historic data is imported from each prefix with setting of 'import' and skip existing records setting of 'true'

  @admin-corporate-data-load
  Scenario: Corporate data loader is run on ingest hase EMR cluster to load data to HBase
    Given A bash 'download cdl script' step is started on the ingest-hbase EMR cluster
    And The 'download cdl script' step is executed successfully
    When The corporate data is loaded in to HBase with default settings
    Then The data load is completed successfully with timeout setting of 'false'

  @admin-historic-data-load
  Scenario: Historic data loader is run on ingest hase EMR cluster to load data to HBase
    Given A bash 'download hdl script' step is started on the ingest-hbase EMR cluster
    And The 'download hdl script' step is executed successfully   
    When The historic data is loaded in to HBase with arguments of 'default'
    Then The data load is completed successfully with timeout setting of 'false'
