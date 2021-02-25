@data-export
@fixture.k2hb.start
@fixture.htme.start.full
@fixture.historic.data.importer.start
@test
Feature: UCFS Business Data Export to Snapshots

    @fixture.htme.start.incremental
    @fixture.snapshot.sender.start.max
    @fixture.s3.clear.snapshot
    @fixture.s3.clear.incremental.snapshot.output
    @fixture.hbase.clear.ingest.unique.start
    @fixture.dynamodb.clear.ingest.start.unique.incremental
    Scenario Outline: Messages are exported to incremental snapshots when using custom start and end times
        Given The topics are configured with unique names
        When UCFS send a message of type 'kafka_main' to each topic with date of '<date>' and key of '<key>'
        And The latest timestamped 'kafka_main' message has been stored in HBase unaltered with id format of 'unwrapped'
        And The export process is performed on the unique topics with start time of '<search-start>', end time of '<search-end>' and snapshot type of 'incremental'
        Then The number of snapshots created for each topic is '<snapshots-expected-count>' with match type of 'exact' and snapshot type of 'incremental'
        Examples: Messages
            | date                    | key                                  | search-start                 | search-end                    | snapshots-expected-count |
            | 2017-10-03T05:04:03.003 | 145a3bb3-d355-4dd8-a764-baec742a426a | 2017-10-03T05:04:03.000000   | 2017-10-03T05:05:03.000000    | 1                        |
            | 2017-10-04T05:04:03.003 | 757f734f-2167-4960-8260-83a0ff782213 | 2017-10-03T05:04:03.000000   | 2017-10-03T05:04:03.000000    | 0                        |
