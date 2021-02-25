Feature: Generation of data

    @generate-historic-data
    Scenario: Historic data is generated and sent up to S3 from environment variable settings
        Given UCFS upload files for each of the given template files with type of 'output'
            | input-file-name-import  | output-file-name-import | snapshot-record-file-name-import |
            | input_template.json     | output_template.json    | snapshot_record_valid.json       |

    @generate-corporate-data
    Scenario: Corporate data is generated and sent up to S3 from environment variable settings
        Given We generate corporate data for each of the given template files
            | input-file-name-corporate       | output-file-name-corporate      |
            | current_valid_file_input.json   | current_valid_file_output.json  |
