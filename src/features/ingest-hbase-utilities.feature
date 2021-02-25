Feature: Utilities to work with the ingest hbase emr cluster

  @admin-ingest-hbase-stop-cleaner-chore
  Scenario: Maintenance window stop activities are performed
    When A bash 'disable cleaner chore' step is started on the ingest-hbase EMR cluster
    And The 'disable cleaner chore' step is executed successfully

  @admin-ingest-hbase-start-cleaner-chore
  Scenario: Maintenance window start activities are performed
    When A bash 'enable cleaner chore' step is started on the ingest-hbase EMR cluster
    And The 'enable cleaner chore' step is executed successfully

  @admin-ingest-hbase-stop-balancer
  Scenario: Maintenance window stop activities are performed
    When A bash 'disable balancer' step is started on the ingest-hbase EMR cluster
    And The 'disable balancer' step is executed successfully

  @admin-ingest-hbase-start-balancer
  Scenario: Maintenance window start activities are performed
    When A bash 'enable balancer' step is started on the ingest-hbase EMR cluster
    And The 'enable balancer' step is executed successfully

  @admin-ingest-hbase-clear-down
  Scenario: All tables are disabled then deleted on ingest hase EMR cluster
    When A bash 'disable all tables' step is started on the ingest-hbase EMR cluster
    And The 'disable all tables' step is executed successfully
    And A bash 'drop all tables' step is started on the ingest-hbase EMR cluster
    And The 'drop all tables' step is executed successfully

  @admin-ingest-hbase-disable-tables
  Scenario: All tables are disabled on ingest hase EMR cluster
    When A bash 'disable all tables' step is started on the ingest-hbase EMR cluster
    And The 'disable all tables' step is executed successfully

  @admin-ingest-hbase-major-compaction
  Scenario: A major compaction of all tables is triggered on ingest hase EMR cluster
    When A script 'download scripts' step is started on the ingest-hbase EMR cluster
    And The 'download scripts' step is executed successfully
    And A script 'major compaction' step is started on the ingest-hbase EMR cluster
    And The 'major compaction' step is executed successfully

  @admin-ingest-hbase-hbck
  Scenario: A hbck on all tables is triggered on ingest hase EMR cluster
    When A script 'download scripts' step is started on the ingest-hbase EMR cluster
    And The 'download scripts' step is executed successfully
    And A script 'hbck' step is started on the ingest-hbase EMR cluster
    And The 'hbck' step is executed successfully

  @admin-ingest-hbase-sync-emrfs
  Scenario: An EMRFS sync is triggered on ingest hase EMR cluster
    When An emrfs 'sync' step is started on the ingest-hbase EMR cluster
    And The 'sync' step is executed successfully

  @admin-ingest-hbase-full-emrfs
  Scenario: An EMRFS delete, import and sync is triggered on ingest hase EMR cluster
    When An emrfs 'delete' step is sent to the ingest-hbase EMR cluster
    And The 'delete' step is executed successfully
    And An emrfs 'import' step is sent to the ingest-hbase EMR cluster
    And The 'import' step is executed successfully
    And An emrfs 'sync' step is sent to the ingest-hbase EMR cluster
    And The 'sync' step is executed successfully

  @admin-ingest-hbase-generate-snapshots
  Scenario: Snapshots generated for ingest hbase emr tables and remove ones older than a month
    When A script 'download scripts' step is started on the ingest-hbase EMR cluster
    And The 'download scripts' step is executed successfully
    And A script 'generate snapshots' step is started on the ingest-hbase EMR cluster
    And The 'generate snapshots' step is executed successfully
