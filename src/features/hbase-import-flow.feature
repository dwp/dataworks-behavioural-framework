@hbase-import-flow
@test
Feature: HBASE Snapshot Import Flow Test

  Scenario: We can download HBASE Snapshot Import Script
    When The HBASE Snapshot Import script is downloaded on the ingest-hbase EMR cluster
    Then The Download HBASE Import script step is executed successfully

  Scenario: We can download HBASE Snapshot Restore Script
    When The HBASE Snapshot Restore script is downloaded on the ingest-hbase EMR cluster
    Then The Download HBASE Restore script step is executed successfully

  @fixture.clean.up.hbase.export.hbase.snapshots
  Scenario: We can import HBASE Snapshots and restore HBASE tables from them
    When The HBASE Snapshot 'automated_tests_snapshot' is imported from the HBASE Export Bucket to the HBASE root dir
    Then The imported HBASE Snapshot 'automated_tests_snapshot' is restored

  @fixture.clean.up.hbase.export.hbase.snapshots
  @fixture.clean.up.hbase.snapshot.cloned.table
  Scenario: We can import HBASE Snapshots and clone HBASE tables from them
    When The HBASE Snapshot 'automated_tests_snapshot' is imported from the HBASE Export Bucket to the HBASE root dir
    Then The imported HBASE Snapshot 'automated_tests_snapshot' is cloned into a HBASE table