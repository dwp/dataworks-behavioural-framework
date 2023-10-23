@data-egress-s3
Feature: Data egress transfer txr data to S3 end to end test

  Scenario: SFT service to transfer txr data to S3 destination end to end test
    Given a set of collections:
        |   name                                        |   destination     |
        |   db.core.statement                           |   ris,cre         |
        |   db.core.claimant                            |   ris,cre         |
        |   db.core.contract                            |   ris,cre         |
        |   db.accepted-data.address                    |   ris,cre         |
        |   db.accepted-data.childrenCircumstances      |   ris,cre         |
        |   db.accepted-data.personDetails              |   ris,cre         |
        |   db.crypto.encryptedData-unencrypted         |   ris,cre         |
        |   db.organisation.organisation                |   ris,cre         |
        |   db.agent-core.agent                         |   ris,cre         |
        |   db.core.todo                                |   ris,!cre        |
        |   db.accepted-data.other                      |   ris,!cre        |
        |   db.crypto.encryptedData                     |   ris,!cre        |
        |   db.calculation.calculationParts             |   ris,!cre        |
    When we submit them to the 'RIS' data directory on the SFT service
    Then we verify the collection files are correctly distributed in S3
