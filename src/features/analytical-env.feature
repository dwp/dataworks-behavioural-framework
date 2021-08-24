@analytical
@fixture.analytical.setup
@test
Feature: Creating Analytical Environment for End Users

  Scenario: Confirm analytical environment E2E tests are being invoked
    When The analytical environment is setup

#  Commented until decided if they will be used - currently failing due to being run without environment spun up.
#  Scenario: A non-PII user is attempting to access non-PII and PII data
#    Given PII and non-PII is added to the output S3 bucket and a user is not authorised to access PII
#    When the user attempts to access PII and non PII data
#    Then the user is forbidden from accessing PII data but can access non PII
#
#  Scenario: A PII user is attempting to access non-PII and PII data
#    Given PII and non-PII is added to the output S3 bucket and a user is authorised to access PII
#    When the user attempts to access PII and non PII data
#    Then the user is able to access PII data and non PII data

#  Scenario: Concourse user is attempting to read a file from s3 location
#    Given S3 file exists
#    When the concourse user attempts to read the file
#    Then the user is able to read the file from S3
#
#  @fixture.setup.non_sc_user_and_role
#  Scenario: A non SC cleared user attempts to access PII data in the published S3 bucket
#    Given A user is not cleared to access PII data in the published S3 bucket
#    When The user Attempts to read data tagged with the pii:true tag in the published S3 bucket
#    Then The user is unable to read the data
#
#  @fixture.setup.non_sc_user
#  Scenario: A non SC cleared user attempts to access non PII data in the published S3 bucket
#    Given A user is cleared to access non PII data in the published S3 bucket
#    When The user Attempts to read data tagged with the pii:false tag in the published S3 bucket
#    Then The user is able to read the data
#
#  @fixture.setup.non_sc_user
#  Scenario: A Non SC User not in any team attempts to access data from the team directories in the published S3 bucket
#    Given A user is not cleared to access any data in any team databases in the published S3 bucket
#    When The user attempts to read data from each of the team directories in the published S3 bucket
#    Then The user is unable to read any of the data
#
#  @fixture.setup.non_sc_user
#  @fixture.cleanup.role_and_s3
#  Scenario: A Non SC User not in any team attempts to access non PII data from the team directories in the published S3 bucket
#    Given A user is not cleared to access any non PII data in any team databases in the published S3 bucket
#    When The user attempts to read data from each of the team directories in the published S3 bucket
#    Then The user is unable to read any of the data
#
#  @fixture.setup.sc_user_and_role
#  Scenario: An SC cleared user attempts to access PII data in the published S3 bucket
#    Given A user is cleared to access PII data in the published S3 bucket
#    When The user Attempts to read data tagged with the pii:true tag in the published S3 bucket
#    Then The user is able to read the data
#
#  @fixture.setup.sc_user
#  Scenario: An SC cleared user attempts to access non PII data in the published S3 bucket
#    Given A user is cleared to access non PII data in the published S3 bucket
#    When The user Attempts to read data tagged with the pii:false tag in the published S3 bucket
#    Then The user is able to read the data
#
#  @fixture.setup.sc_user
#  @fixture.cleanup.role_and_s3
#  Scenario: An SC cleared user attempts to access data in the restricted directories in the published S3 bucket
#    Given A user is not cleared to access data in the restricted directories in the published S3 bucket
#    When The user Attempts to read data from each of the restricted directories in the published S3 bucket
#    Then The user is unable to read any of the data
#
#  @fixture.setup.auditlog_secure_user_and_role
#  Scenario: An auditlog_secure user attempts to access PII data in the auditlog_red_v table in the published S3 bucket
#    Given A user is not cleared to access data in the auditlog_red_v table in the published S3 bucket
#    When The user attempts to read data from the auditlog_red_v table in the published S3 bucket
#    Then The user is unable to read the data
#
#  @fixture.setup.auditlog_secure_user
#  @fixture.cleanup.role_and_s3
#  Scenario: An auditlog_secure user attempts to access PII data in the auditlog_sec_v table in the published S3 bucket
#    Given A user is cleared to access data in the auditlog_sec_v table in the published S3 bucket
#    When The user attempts to read data from the auditlog_sec_v table in the published S3 bucket
#    Then The user is able to read the data
#
#  @fixture.setup.auditlog_unredacted_user_and_role
#  Scenario: An auditlog_unredacted user attempts to access PII data in the auditlog_red_v table in the published S3 bucket
#    Given A user is cleared to access data in the auditlog_red_v table in the published S3 bucket
#    When The user attempts to read data from the auditlog_red_v table in the published S3 bucket
#    Then The user is able to read the data
#
#  @fixture.setup.auditlog_unredacted_user
#  Scenario: An auditlog_unredacted user attempts to access PII data in the auditlog_sec_v table in the published S3 bucket
#    Given A user is cleared to access data in the auditlog_sec_v table in the published S3 bucket
#    When The user attempts to read data from the auditlog_sec_v table in the published S3 bucket
#    Then The user is able to read the data
#
#  @fixture.setup.auditlog_unredacted_user
#  @fixture.cleanup.role_and_s3
#  Scenario: An auditlog_unredacted user attempts to access PII data in the auditlog_unred_v table in the published S3 bucket
#    Given A user is cleared to access data in the auditlog_unred_v table in the published S3 bucket
#    When The user attempts to read data from the auditlog_unred_v table in the published S3 bucket
#    Then The user is able to read the data
#
#  @fixture.setup.auditlog_redacted_user_and_role
#  @fixture.cleanup.role_and_s3
#  Scenario: An auditlog_redacted user attempts to access PII data in the auditlog_red_v table in the published S3 bucket
#    Given A user is cleared to access data in the auditlog_red_v table in the published S3 bucket
#    When The user attempts to read data from the auditlog_red_v table in the published S3 bucket
#    Then The user is able to read the data
#
#  @fixture.setup.rbac_ucs_opsmi_redacted__all_and_role
#  Scenario: A UCS OpsMI Redacted team user attempts to access data in the ucs_opsmi_redacted table in the published S3 bucket
#    Given A user is only cleared to access ucs_opsmi_redacted table in the published S3 bucket
#    When The user attempts to read data from the their team table in the published S3 bucket
#    Then The user is able to read the data
#
#  @fixture.setup.rbac_ucs_opsmi_redacted__all
#  Scenario: A UCS OpsMI Redacted team user attempts to access data in another table in the published S3 bucket
#    Given A user is not cleared to access other tables than ucs_opsmi_redacted table in the published S3 bucket
#    When The user attempts to read data from another table in the published S3 bucket
#    Then The user is unable to read the data
#
#  @fixture.setup.rbac_ucs_opsmi_redacted__all
#  Scenario: A UCS OpsMI Redacted team user attempts to access PII data from the UC database in the published S3 bucket
#    Given A user is not cleared to access PII data from the UC database in the published S3 bucket
#    When The user Attempts to read data tagged with the pii:true tag from the UC database in the published S3 bucket
#    Then The user is unable to read the data
#
#  @fixture.setup.rbac_ucs_opsmi_redacted__all
#  @fixture.cleanup.role_and_s3
#  Scenario: A UCS OpsMI Redacted team user attempts to access non PII data from the UC database in the published S3 bucket
#    Given A user is not cleared to access non PII data from the UC database in the published S3 bucket
#    When The user Attempts to read data tagged with the pii:false tag from the UC database in the published S3 bucket
#    Then The user is unable to read the data
#
#  @fixture.setup.rbac_ucs_opsmi_unredacted__all_and_role
#  Scenario: A UCS OpsMI Unredacted team user attempts to access data in the ucs_opsmi_redacted table in the published S3 bucket
#    Given A user is only cleared to access ucs_opsmi_unredacted table in the published S3 bucket
#    When The user attempts to read data from the their team table in the published S3 bucket
#    Then The user is able to read the data
#
#  @fixture.setup.rbac_ucs_opsmi_unredacted__all
#  Scenario: A UCS OpsMI Unredacted team user attempts to access data in another table in the published S3 bucket
#    Given A user is not cleared to access other tables than ucs_opsmi_unredacted table in the published S3 bucket
#    When The user attempts to read data from another table in the published S3 bucket
#    Then The user is unable to read the data
#
#  @fixture.setup.rbac_ucs_opsmi_unredacted__all
#  Scenario: A UCS OpsMI Unredacted team user attempts to access PII data from the UC database in the published S3 bucket
#    Given A user is not cleared to access PII data from the UC database in the published S3 bucket
#    When The user Attempts to read data tagged with the pii:true tag from the UC database in the published S3 bucket
#    Then The user is unable to read the data
#
#  @fixture.setup.rbac_ucs_opsmi_unredacted__all
#  @fixture.cleanup.role_and_s3
#  Scenario: A UCS OpsMI Unredacted team user attempts to access non PII data from the UC database in the published S3 bucket
#    Given A user is not cleared to access non PII data from the UC database in the published S3 bucket
#    When The user Attempts to read data tagged with the pii:false tag from the UC database in the published S3 bucket
#    Then The user is unable to read the data
#
#  @fixture.setup.rbac_uc_ris_redacted__all_and_role
#  Scenario: A UC RIS Redacted team user attempts to access data in the uc_ris_redacted table in the published S3 bucket
#    Given A user is only cleared to access uc_ris_redacted table in the published S3 bucket
#    When The user attempts to read data from the their team table in the published S3 bucket
#    Then The user is able to read the data
#
#  @fixture.setup.rbac_uc_ris_redacted__all
#  Scenario: A UC RIS Redacted team user attempts to access data in another table in the published S3 bucket
#    Given A user is not cleared to access other tables than uc_ris_redacted table in the published S3 bucket
#    When The user attempts to read data from another table in the published S3 bucket
#    Then The user is unable to read the data
#
#  @fixture.setup.rbac_uc_ris_redacted__all
#  Scenario: A UC RIS Redacted team user attempts to access PII data from the UC database in the published S3 bucket
#    Given A user is not cleared to access PII data from the UC database in the published S3 bucket
#    When The user Attempts to read data tagged with the pii:true tag from the UC database in the published S3 bucket
#    Then The user is unable to read the data
#
#  @fixture.setup.rbac_uc_ris_redacted__all
#  @fixture.cleanup.role_and_s3
#  Scenario: A UC RIS Redacted team user attempts to access non PII data from the UC database in the published S3 bucket
#    Given A user is not cleared to access non PII data from the UC database in the published S3 bucket
#    When The user Attempts to read data tagged with the pii:false tag from the UC database in the published S3 bucket
#    Then The user is unable to read the data
#
#  @fixture.setup.rbac_uc_ris_unredacted__all_and_role
#  Scenario: A UC RIS Redacted team user attempts to access data in the uc_ris_redacted table in the published S3 bucket
#    Given A user is only cleared to access uc_ris_unredacted table in the published S3 bucket
#    When The user attempts to read data from the their team table in the published S3 bucket
#    Then The user is able to read the data
#
#  @fixture.setup.rbac_uc_ris_unredacted__all
#  Scenario: A UC RIS Redacted team user attempts to access data in another table in the published S3 bucket
#    Given A user is not cleared to access other tables than uc_ris_unredacted table in the published S3 bucket
#    When The user attempts to read data from another table in the published S3 bucket
#    Then The user is unable to read the data
#
#  @fixture.setup.rbac_uc_ris_unredacted__all
#  Scenario: A UC RIS Unredacted team user attempts to access PII data from the UC database in the published S3 bucket
#    Given A user is not cleared to access PII data from the UC database in the published S3 bucket
#    When The user Attempts to read data tagged with the pii:true tag from the UC database in the published S3 bucket
#    Then The user is unable to read the data
#
#  @fixture.setup.rbac_uc_ris_unredacted__all
#  @fixture.cleanup.role_and_s3
#  Scenario: A UC RIS Unredacted team user attempts to access non PII data from the UC database in the published S3 bucket
#    Given A user is not cleared to access non PII data from the UC database in the published S3 bucket
#    When The user Attempts to read data tagged with the pii:false tag from the UC database in the published S3 bucket
#    Then The user is unable to read the data
#
#  @fixture.setup.rbac_uc__auditlog_redacted_and_sc_and_role
#  @fixture.cleanup.role_and_s3
#  Scenario: A user with auditlog redacted and SC access attempts to access PII data in the published S3 bucket
#    Given A user is cleared to access PII data in the published S3 bucket
#    When The user attempts to read data tagged with the pii:true tag in the published S3 bucket
#    Then The user is able to read the data
#
#  @fixture.setup.rbac_uc__auditlog_redacted_and_non_sc_and_role
#  Scenario: A user with auditlog redacted and Non SC access attempts to access non PII data in the published S3 bucket
#    Given A user is cleared to access non PII data in the published S3 bucket
#    When The user attempts to read data tagged with the pii:false tag in the published S3 bucket
#    Then The user is able to read the data
#
#  @fixture.setup.rbac_uc__auditlog_redacted_and_non_sc
#  @fixture.cleanup.role_and_s3
#  Scenario: A user with auditlog redacted and Non SC access attempts to access non PII data in the published S3 bucket
#    Given A user is not cleared to access PII data in the published S3 bucket
#    When The user attempts to read data tagged with the pii:true tag in the published S3 bucket
#    Then The user is unable to read the data
#
#  @fixture.setup.rbac_uc__auditlog_redacted_and_auditlog_secure_and_role
#  Scenario: A user with auditlog redacted and auditlog secure access data in the auditlog_sec_v table in the published S3 bucket
#    Given A user is cleared to access data in the auditlog_sec_v table in the published S3 bucket
#    When The user attempts to read data from the auditlog_sec_v table in the published S3 bucket
#    Then The user is able to read the data
#
#  @fixture.setup.rbac_uc__auditlog_redacted_and_auditlog_secure
#  @fixture.cleanup.role_and_s3
#  Scenario: A user with auditlog redacted and auditlog secure access attempts to access data in the auditlog_unred_v table in the published S3 bucket
#    Given A user is not cleared to access data in the auditlog_unred_v table in the published S3 bucket
#    When The user attempts to read data from the auditlog_unred_v table in the published S3 bucket
#    Then The user is unable to read the data
#
#  @fixture.setup.rbac_uc__sc_and_non_sc_and_role
#  Scenario: A user with SC and Non SC access attempts to access non PII data in the published S3 bucket
#    Given A user is cleared to access non PII data in the published S3 bucket
#    When The user attempts to read data tagged with the pii:false tag in the published S3 bucket
#    Then The user is able to read the data
#
#  @fixture.setup.rbac_uc__sc_and_non_sc
#  Scenario: A user with SC and Non SC access attempts to access PII data in the published S3 bucket
#    Given A user is cleared to access PII data in the published S3 bucket
#    When The user attempts to read data tagged with the pii:true tag in the published S3 bucket
#    Then The user is able to read the data
#
#  @fixture.setup.rbac_uc__sc_and_non_sc
#  Scenario: A user with SC and Non SC access attempts to access data in the auditlog_red_v table in the published S3 bucket
#    Given A user is not cleared to access data in the auditlog_red_v table in the published S3 bucket
#    When The user attempts to read data from the auditlog_red_v table in the published S3 bucket
#    Then The user is unable to read the data
#
#  @fixture.setup.rbac_uc__sc_and_non_sc
#  Scenario: A user with SC and Non SC access attempts to access data in the auditlog_unred_v table in the published S3 bucket
#    Given A user is not cleared to access data in the auditlog_unred_v table in the published S3 bucket
#    When The user attempts to read data from the auditlog_unred_v table in the published S3 bucket
#    Then The user is unable to read the data
#
#  @fixture.setup.rbac_uc__sc_and_non_sc
#  @fixture.cleanup.role_and_s3
#  Scenario: A user with SC and Non SC access attempts to access data in the auditlog_sec_v table in the published S3 bucket
#    Given A user is not cleared to access data in the auditlog_sec_v table in the published S3 bucket
#    When The user attempts to read data from the auditlog_sec_v table in the published S3 bucket
#    Then The user is unable to read the data
#
#  @fixture.setup.rbac_uc_mongo_latest__uc_mongo_latest__full_access_and_role
#  Scenario: A user with read access to uc_mongo_latest DB attempts to access data in the published S3 bucket location
#    Given A user is cleared to read uc_mongo_latest DB data in the published S3 bucket
#    When The user attempts to read data in the published S3 bucket location
#    Then The user is able to read the data
#
#  @fixture.setup.rbac_uc_mongo_latest__uc_mongo_latest__full_access
#  @fixture.cleanup.s3
#  Scenario: A user with write access to uc_mongo_latest DB attempts to write data in the published S3 bucket location
#    Given A user is cleared to write to uc_mongo_latest DB location in the published S3 bucket
#    When The user attempts to write to the published S3 bucket location
#    Then The user is able to write to the location
#
#  @fixture.setup.rbac_uc_mongo_latest__uc_mongo_latest__full_access
#  @fixture.cleanup.role_and_s3
#  Scenario: A user with write access to uc_mongo_latest DB attempts to write data in another S3 bucket location
#    Given A user is only cleared to write to the uc_mongo_latest DB location in the published S3 bucket
#    When The user attempts to write to another S3 bucket location
#    Then The user is unable to write to the location
#
#  @fixture.setup.rbac_uc_mongo_latest__uc_mongo_latest__read_access_and_role
#  Scenario: A user with read access to uc_mongo_latest DB attempts to access data in the published S3 bucket location
#    Given A user is cleared to read uc_mongo_latest DB data in the published S3 bucket
#    When The user attempts to read data in the published S3 bucket location
#    Then The user is able to read the data
#
#  @fixture.setup.rbac_uc_mongo_latest__uc_mongo_latest__read_access
#  @fixture.cleanup.role_and_s3
#  Scenario: A user with read only access to uc_mongo_latest DB attempts to access data in the published S3 bucket location
#    Given A user is not cleared to write to uc_mongo_latest DB location in the published S3 bucket
#    When The user attempts to write to the published S3 bucket location
#    Then The user is unable to write to the location
#
#  @fixture.setup.rbac_ucs_latest_redacted__ucs_latest_redacted__full_access_and_role
#  @fixture.cleanup.s3
#  Scenario: A user with write access to ucs_latest_redacted DB attempts to write data in the published S3 bucket location
#    Given A user is cleared to write to ucs_latest_redacted DB location in the published S3 bucket
#    When The user attempts to write to the published S3 bucket location
#    Then The user is able to write to the location
#
#  @fixture.setup.rbac_ucs_latest_redacted__ucs_latest_redacted__full_access
#  @fixture.cleanup.role_and_s3
#  Scenario: A user with write access to ucs_latest_redacted DB attempts to access data in another S3 bucket location
#    Given A user is only cleared to write to the ucs_latest_redacted DB location in the published S3 bucket
#    When The user attempts to write to another S3 bucket location
#    Then The user is unable to write to the location
#
#  @fixture.setup.rbac_ucs_latest_redacted__ucs_latest_redacted__read_access_and_role
#  Scenario: A user with read access to ucs_latest_redacted DB attempts to access data in the published S3 bucket location
#    Given A user is cleared to read ucs_latest_redacted DB data in the published S3 bucket
#    When The user attempts to read data in the published S3 bucket location
#    Then The user is able to read the data
#
#  @fixture.setup.rbac_ucs_latest_redacted__ucs_latest_redacted__read_access
#  @fixture.cleanup.role_and_s3
#  Scenario: A user with read only access to ucs_latest_redacted DB attempts to write data in the published S3 bucket location
#    Given A user is not cleared to write to ucs_latest_redacted DB location in the published S3 bucket
#    When The user attempts to write to the published S3 bucket location
#    Then The user is unable to write to the location
#
#  @fixture.setup.rbac_ucs_latest_unredacted__ucs_latest_unredacted__full_access_and_role
#  Scenario: A user with write access to ucs_latest_unredacted DB attempts to access data in the published S3 bucket location
#    Given A user is cleared to read ucs_latest_unredacted DB data in the published S3 bucket
#    When The user attempts to read data in the published S3 bucket location
#    Then The user is able to read the data
#
#  @fixture.setup.rbac_ucs_latest_unredacted__ucs_latest_unredacted__full_access
#  @fixture.cleanup.s3
#  Scenario: A user with write access to ucs_latest_unredacted DB attempts to write data in the published S3 bucket location
#    Given A user is cleared to write to ucs_latest_unredacted DB location in the published S3 bucket
#    When The user attempts to write to the published S3 bucket location
#    Then The user is able to write to the location
#
#  @fixture.setup.rbac_ucs_latest_unredacted__ucs_latest_unredacted__full_access
#  @fixture.cleanup.role_and_s3
#  Scenario: A user with write access to ucs_latest_unredacted DB attempts to access data in another S3 bucket location
#    Given A user is only cleared to write to the ucs_latest_unredacted DB location in the published S3 bucket
#    When The user attempts to write to another S3 bucket location
#    Then The user is unable to write to the location
#
#  @fixture.setup.rbac_ucs_latest_unredacted__ucs_latest_unredacted__read_access_and_role
#  Scenario: A user with read access to ucs_latest_unredacted DB attempts to access data in the published S3 bucket location
#    Given A user is cleared to read ucs_latest_unredacted DB data in the published S3 bucket
#    When The user attempts to read data in the published S3 bucket location
#    Then The user is able to read the data
#
#  @fixture.setup.rbac_ucs_latest_unredacted__ucs_latest_unredacted__read_access
#  @fixture.cleanup.role_and_s3
#  Scenario: A user with read only access to ucs_latest_unredacted DB attempts to write data in the published S3 bucket location
#    Given A user is not cleared to write to ucs_latest_unredacted DB location in the published S3 bucket
#    When The user attempts to write to the published S3 bucket location
#    Then The user is unable to write to the location
#
#  @fixture.setup.rbac_uc_clive__non_pii_user_and_role
#  Scenario: A Non SC Clive user attempts to access non PII clive DB data in the published S3 bucket
#    Given A user is cleared to read non PII clive DB data in the published S3 bucket
#    When The user Attempts to read data tagged with the pii:false tag from the Clive database in the published S3 bucket
#    Then The user is able to read the data
#
#  @fixture.setup.rbac_uc_clive__non_pii_user
#  Scenario: A Non SC Clive user attempts to access PII clive DB data in the published S3 bucket
#    Given A user is not cleared to read PII clive DB data in the published S3 bucket
#    When The user Attempts to read data tagged with the pii:true tag from the Clive database in the published S3 bucket
#    Then The user is unable to read the data
#
#  @fixture.setup.rbac_uc_clive__non_pii_user
#  @fixture.cleanup.role_and_s3
#  Scenario: A Non SC Clive user attempts to access Non PII data from the UC DB in the published S3 bucket
#    Given A user is not cleared to access non PII data from the UC database in the published S3 bucket
#    When The user Attempts to read data tagged with the pii:false tag in the published S3 bucket
#    Then The user is unable to read the data
#
#  @fixture.setup.rbac_uc_clive__pii_user_and_role
#  Scenario: An SC Clive user attempts to access non PII clive DB data in the published S3 bucket
#    Given A user is cleared to read non PII clive DB data in the published S3 bucket
#    When The user Attempts to read data tagged with the pii:false tag from the Clive database in the published S3 bucket
#    Then The user is able to read the data
#
#  @fixture.setup.rbac_uc_clive__pii_user
#  Scenario: An SC Clive user attempts to access PII clive DB data in the published S3 bucket
#    Given A user is cleared to read PII clive DB data in the published S3 bucket
#    When The user Attempts to read data tagged with the pii:true tag from the Clive database in the published S3 bucket
#    Then The user is able to read the data
#
#  @fixture.setup.rbac_uc_clive__pii_user
#  Scenario: An SC Clive user attempts to access PII data from the UC DB in the published S3 bucket
#    Given A user is not cleared to access PII data from the UC database in the published S3 bucket
#    When The user Attempts to read data tagged with the pii:true tag in the published S3 bucket
#    Then The user is unable to read the data
#
#  @fixture.setup.rbac_uc_clive__pii_user
#  Scenario: An SC Clive user attempts to access Non PII data from the UC DB in the published S3 bucket
#    Given A user is not cleared to access non PII data from the UC database in the published S3 bucket
#    When The user Attempts to read data tagged with the pii:false tag in the published S3 bucket
#    Then The user is unable to read the data
#
#  @fixture.setup.rbac_uc_clive__pii_user
#  Scenario: An SC Clive user attempts to access data in the uc_mongo_latest DB in the published S3 bucket
#    Given A user is not cleared to read uc_mongo_latest DB data in the published S3 bucket
#    When The user attempts to read data in the published S3 bucket location
#    Then The user is unable to read the data
#
#  @fixture.setup.rbac_uc_clive__pii_user
#  Scenario: An SC Clive user attempts to access data in the uc_lab DB in the published S3 bucket
#    Given A user is not cleared to read uc_lab DB data in the published S3 bucket
#    When The user attempts to read data in the published S3 bucket location
#    Then The user is unable to read the data
#
#  @fixture.setup.rbac_uc_clive__pii_user
#  @fixture.cleanup.role_and_s3
#  Scenario: An SC Clive user attempts to access data in the equality DB in the published S3 bucket
#    Given A user is not cleared to read equality DB data in the published S3 bucket
#    When The user attempts to read data in the published S3 bucket location
#    Then The user is unable to read the data
#
  @fixture.setup.rbac_uc_equality__pii_user_and_role
  Scenario: An SC equality user attempts to access PII equality DB data in the published S3 bucket
    Given A user is cleared to read equality DB and PII data in the published S3 bucket
    When The user Attempts to read PII data from the equality database in the published S3 bucket
    Then The user is able to read the data

  @fixture.setup.rbac_uc_equality__pii_user
  Scenario: An SC equality user attempts to access data in the uc_lab DB in the published S3 bucket
    Given A user is not cleared to read uc_lab DB data in the published S3 bucket
    When The user attempts to read data in the published S3 bucket location
    Then The user is unable to read the data

  @fixture.setup.rbac_uc_equality__pii_user
  Scenario: A user with read access to uc_equality DB attempts to write data in the published S3 bucket location
    Given A user is not cleared to write to uc_equality DB location in the published S3 bucket
    When The user attempts to write to the published S3 bucket location
    Then The user is unable to write to the location

  @fixture.setup.rbac_uc_equality__pii_user
  @fixture.cleanup.role_and_s3
  Scenario: An SC equality user attempts to access data in the uc_ers DB in the published S3 bucket
    Given A user is not cleared to read uc_ers DB data in the published S3 bucket
    When The user attempts to read data in the published S3 bucket location
    Then The user is unable to read the data

  @fixture.setup.rbac_uc_ers__all_user_and_role
  Scenario: An SC uc_ers user attempts to access PII uc_ers DB data in the published S3 bucket
    Given A user is cleared to read uc_ers DB and PII data in the published S3 bucket
    When The user attempts to read data in the published S3 bucket location
    Then The user is able to read the data

  @fixture.setup.rbac_uc_ers__all_user
  Scenario: An SC uc_ers user attempts to access data in the uc_lab DB in the published S3 bucket
    Given A user is not cleared to read uc_lab DB data in the published S3 bucket
    When The user attempts to read data in the published S3 bucket location
    Then The user is unable to read the data

  @fixture.setup.rbac_uc_ers__all_user
  @fixture.cleanup.role_and_s3
  Scenario: A user with write access to uc_ers DB attempts to write data in the published S3 bucket location
    Given A user is cleared to write to uc_ers DB location in the published S3 bucket
    When The user attempts to write to the published S3 bucket location
    Then The user is able to write to the location

