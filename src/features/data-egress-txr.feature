@data-egress-s3
Feature: Data egress transfer txr data to S3 end to end test

  Scenario: SFT service to transfer transaction data to HTTP destination end to end test
    Given a set of collections:
        |   name                                        |   destination     |
        |   db.core.statement                           |   ris             |
        |   db.core.claimant                            |   ris             |
        |   db.core.contract                            |   ris             |
        |   db.accepted-data.address                    |   ris             |
        |   db.accepted-data.childrenCircumstances      |   ris             |
        |   db.accepted-data.personDetails              |   ris             |
        |   db.crypto.encryptedData-unencrypted         |   ris             |
        |   db.core.todo                                |   cre             |
        |   db.accepted-data.other                      |   cre             |
        |   db.cryto.encryptedData                      |   cre             |
        |   db.calculation.calculationParts             |   cre             |
    When we submit them to the 'ris-tmp' data directory on the SFT service
    Then we verify the collection files are correctly distributed in S3
