@analytical-dataset-generation
@test
Feature: Analytical data set generation end to end test

    @fixture.s3.clear.snapshot.start
    @fixture.terminate.adg.cluster
    Scenario: Analytical data set generation end to end test for full
      Given the data of the format in the template file 'snapshot_record_valid.json' as an input to analytical data set generation emr
      Then start adg 'full' cluster and wait for the step 'sns-notification'
      And read metadata of the analytical data sets from the path 'analytical-dataset/full/adg_output/adg_params.csv'
      And verify metadata, tags of the analytical data sets for 'full'
      And the ADG cluster tags have been created correctly for 'full'
      And the ADG metadata table is correct for 'full'

    @fixture.s3.clear.snapshot.start
    @fixture.terminate.adg.cluster
    Scenario: Analytical data set generation end to end test for incremental
      Given the data of the format in the template file 'snapshot_record_valid_incremental.json' as an input to analytical data set generation emr
      Then start adg 'incremental' cluster and wait for the step 'flush-pushgateway'
      And read metadata of the analytical data sets from the path 'analytical-dataset/incremental/adg_output/adg_params.csv'
      And verify metadata, tags of the analytical data sets for 'incremental'
      And the ADG cluster tags have been created correctly for 'incremental'
      And the ADG metadata table is correct for 'incremental'
