Feature: Administrative processes and workflows

  @admin-generate-full-snapshots
  @fixture.htme.start.full
  Scenario: Start an export from HBase to snapshot files on S3 for the full snapshot topics
    Given Snapshot sender is scaled if it will be triggered for snapshot type of 'full'
    When The export process is performed with default settings for snapshot type of 'full'

  @admin-generate-incremental-snapshots
  @fixture.htme.start.incremental
  Scenario: Start an export from HBase to snapshot files on S3 for the incremental snapshot topics
    Given Snapshot sender is scaled if it will be triggered for snapshot type of 'incremental'
    When The export process is performed with default settings for snapshot type of 'incremental'

  @admin-generate-drift-testing-incremental-snapshots
  @fixture.htme.start.drift.testing.incremental
  Scenario: Start an export from HBase to snapshot files on S3 for the incremental snapshot topics
    Given Snapshot sender is scaled if it will be triggered for snapshot type of 'drift_testing_incremental'
    When The export process is performed with default settings for snapshot type of 'drift_testing_incremental'

  @admin-send-full-snapshots-to-crown
  @fixture.snapshot.sender.start.max
  Scenario: Start the sending of S3 full snapshots to Crown
    When The snapshot sending process is performed with default settings for snapshot type of 'full'

  @admin-send-incremental-snapshots-to-crown
  @fixture.snapshot.sender.start.max
  Scenario: Start the sending of S3 incremental snapshots to Crown
    When The snapshot sending process is performed with default settings for snapshot type of 'incremental'

  @admin-send-drift-testing-incremental-snapshots-to-crown
  @fixture.snapshot.sender.start.max
  Scenario: Start the sending of S3 incremental snapshots to Crown
    When The snapshot sending process is performed with default settings for snapshot type of 'drift_testing_incremental'

  @admin-scale-down-hdi
  @fixture.hdi.stop
  Scenario: Stop the HDI instance
    Then The asg has scaled correctly

  @admin-scale-down-htme
  @fixture.htme.stop
  Scenario: Stop the HTME instance
    Then The asg has scaled correctly

  @admin-scale-down-snapshotsender
  @fixture.snapshot.sender.stop
  Scenario: Stop the Snapshot Sender instance
    Then The asg has scaled correctly

  @admin-scale-up-hdi
  @fixture.historic.data.importer.start.max
  Scenario: Start the HDI instance
    Then The asg has scaled correctly

  @admin-scale-up-htme
  @fixture.htme.start.max
  Scenario: Start the HTME instance
    Then The asg has scaled correctly

  @admin-scale-up-snapshotsender
  @fixture.snapshot.sender.start.max
  Scenario: Start the Snapshot Sender instance
    Then The asg has scaled correctly

  @admin-scale-up-kafka-stub
  @fixture.kafka.stub.start
  Scenario: Start the Kafka Broker instance
    Then The asg has scaled correctly

  @admin-scale-down-kafka-stub
  @fixture.kafka.stub.stop
  Scenario: Stop the Kafka Broker instance
    Then The asg has scaled correctly

  @admin-scale-up-k2hb
  @fixture.k2hb.start
  Scenario: Start the K2HB instances
    Then The asg has scaled correctly

  @admin-scale-down-k2hb
  @fixture.k2hb.stop
  Scenario: Stop the K2HB instances
    Then The asg has scaled correctly

  @admin-scale-up-ingestion-ecs-cluster
  @fixture.ingest.ecs.cluster.start
  Scenario: Start the Ingestion ECS cluster instances
    Then The asg has scaled correctly

  @admin-scale-down-ingestion-ecs-cluster
  @fixture.ingest.ecs.cluster.stop
  Scenario: Stop the Ingestion ECS cluster instances
    Then The asg has scaled correctly
