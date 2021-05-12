# UCFS Claimant API

Claimants have assessment periods associated with themselves. This can be one or many. These periods span a period of time and have a take home pay characteristic. An assessment period is also known as a claim.
A claimant can have their claim suspended or closed.

To test for various scenarios, the 'data' yml files located in `src/fixture-data/kafka_data/claimant_api` create the required JSON bodies which are inserted into the database.

## Creating a new scenario
When creating a new scenario, you can take advantage of the following options in the creation of your scenario yaml file, to configure the claimants assessment periods as required:

`start_date` - a string date value in YYYYMMDD format to be used as is, in the data.

`end_date` - a string date value in YYYYMMDD format to be used as is, in the data.

`start_date_offset` - a string integer value, either positive or negative. The data generator will create the start date based on today +/- value. ie: start_date_offset of -1 if the date when the tests were ran was `20210512`, would result in `20210511`.

`end_date_offset` - a string integer value, either positive or negative. See `start_date_offset`.

`suspended_date` - a string date value in YYYYMMDD format to be used as is, in the data.

`suspension_date_offset` - a string integer value, either positive or negative. See `start_date_offset`.

`contract_closed_date` - a string date value in YYYYMMDD format to be used as is, in the data.

`contract_closed_date_offset` -  a string integer value, either positive or negative. See `start_date_offset`.