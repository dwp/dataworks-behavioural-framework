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

  Scenario: Concourse user is attempting to read a file from s3 location
    Given S3 file exists
    When the concourse user attempts to read the file
    Then the user is able to read the file from S3

  @fixture.setup.non_sc_user_and_role
  Scenario: A non SC cleared user attempts to access PII data in the published S3 bucket
    Given A user is not cleared to access PII data in the published S3 bucket
    When The user Attempts to read data tagged with the pii:true tag in the published S3 bucket
    Then The user is unable to read the data

  @fixture.setup.non_sc_user
  Scenario: A non SC cleared user attempts to access non PII data in the published S3 bucket
    Given A user is cleared to access non PII data in the published S3 bucket
    When The user Attempts to read data tagged with the pii:false tag in the published S3 bucket
    Then The user is able to read the data

  @fixture.setup.non_sc_user
  Scenario: A Non SC User not in any team attempts to access data from the team directories in the published S3 bucket
    Given A user is not cleared to access any data in any team databases in the published S3 bucket
    When The user attempts to read data from each of the team directories in the published S3 bucket
    Then The user is unable to read any of the data

  @fixture.setup.non_sc_user
  Scenario: A Non SC User not in any team attempts to access non PII data from the team directories in the published S3 bucket
    Given A user is not cleared to access any non PII data in any team databases in the published S3 bucket
    When The user attempts to read data from each of the team directories in the published S3 bucket
    Then The user is unable to read any of the data

  @fixture.setup.non_sc_user
  @fixture.cleanup.role_and_s3
  Scenario: A non SC cleared user attempts to access data in the restricted directories in the published S3 bucket
    Given A user is not cleared to access data in the restricted directories in the published S3 bucket
    When The user Attempts to read data from each of the restricted directories in the published S3 bucket
    Then The user is unable to read any of the data

  @fixture.setup.sc_user_and_role
  Scenario: An SC cleared user attempts to access PII data in the published S3 bucket
    Given A user is cleared to access PII data in the published S3 bucket
    When The user Attempts to read data tagged with the pii:true tag in the published S3 bucket
    Then The user is able to read the data

  @fixture.setup.sc_user
  Scenario: An SC cleared user attempts to access non PII data in the published S3 bucket
    Given A user is cleared to access non PII data in the published S3 bucket
    When The user Attempts to read data tagged with the pii:false tag in the published S3 bucket
    Then The user is able to read the data

  @fixture.setup.sc_user
  @fixture.cleanup.role_and_s3
  Scenario: An SC cleared user attempts to access data in the restricted directories in the published S3 bucket
    Given A user is not cleared to access data in the restricted directories in the published S3 bucket
    When The user Attempts to read data from each of the restricted directories in the published S3 bucket
    Then The user is unable to read any of the data

  @fixture.setup.auditlog_secure_user_and_role
  Scenario: An auditlog_secure user attempts to access PII data in the auditlog_red_v table in the published S3 bucket
    Given A user is cleared to access data in the auditlog_red_v table in the published S3 bucket
    When The user attempts to read data from the auditlog_red_v table in the published S3 bucket
    Then The user is unable to read the data

  @fixture.setup.auditlog_secure_user
  Scenario: An auditlog_secure user attempts to access PII data in the auditlog_sec_v table in the published S3 bucket
    Given A user is cleared to access data in the auditlog_sec_v table in the published S3 bucket
    When The user attempts to read data from the auditlog_sec_v table in the published S3 bucket
    Then The user is able to read the data

  @fixture.setup.auditlog_secure_user
  @fixture.cleanup.role_and_s3
  Scenario: An auditlog_secure user attempts to access PII data in the auditlog_unred_v table in the published S3 bucket
    Given A user is not cleared to access data in the auditlog_unred_v table in the published S3 bucket
    When The user attempts to read data from the auditlog_unred_v table in the published S3 bucket
    Then The user is unable to read the data

  @fixture.setup.auditlog_unredacted_user_and_role
  Scenario: An auditlog_unredacted user attempts to access PII data in the auditlog_red_v table in the published S3 bucket
    Given A user is cleared to access data in the auditlog_red_v table in the published S3 bucket
    When The user attempts to read data from the auditlog_red_v table in the published S3 bucket
    Then The user is able to read the data

  @fixture.setup.auditlog_unredacted_user
  Scenario: An auditlog_unredacted user attempts to access PII data in the auditlog_sec_v table in the published S3 bucket
    Given A user is cleared to access data in the auditlog_sec_v table in the published S3 bucket
    When The user attempts to read data from the auditlog_sec_v table in the published S3 bucket
    Then The user is able to read the data

  @fixture.setup.auditlog_unredacted_user
  @fixture.cleanup.role_and_s3
  Scenario: An auditlog_unredacted user attempts to access PII data in the auditlog_unred_v table in the published S3 bucket
    Given A user is cleared to access data in the auditlog_unred_v table in the published S3 bucket
    When The user attempts to read data from the auditlog_unred_v table in the published S3 bucket
    Then The user is able to read the data

  @fixture.setup.auditlog_redacted_user_and_role
  Scenario: An auditlog_redacted user attempts to access PII data in the auditlog_red_v table in the published S3 bucket
    Given A user is cleared to access data in the auditlog_red_v table in the published S3 bucket
    When The user attempts to read data from the auditlog_red_v table in the published S3 bucket
    Then The user is able to read the data

  @fixture.setup.auditlog_redacted_user
  Scenario: An auditlog_redacted user attempts to access PII data in the auditlog_sec_v table in the published S3 bucket
    Given A user is not cleared to access data in the auditlog_sec_v table in the published S3 bucket
    When The user attempts to read data from the auditlog_sec_v table in the published S3 bucket
    Then The user is unable to read the data

  @fixture.setup.auditlog_redacted_user
  @fixture.cleanup.role_and_s3
  Scenario: An auditlog_redacted user attempts to access PII data in the auditlog_unred_v table in the published S3 bucket
    Given A user is not cleared to access data in the auditlog_unred_v table in the published S3 bucket
    When The user attempts to read data from the auditlog_unred_v table in the published S3 bucket
    Then The user is unable to read the data

  @fixture.setup.rbac_ucs_opsmi_redacted__all_and_role
  Scenario: A UCS OpsMI Redacted team user attempts to access data in the ucs_opsmi_redacted table in the published S3 bucket
    Given A user is only cleared to access ucs_opsmi_redacted table in the published S3 bucket
    When The user attempts to read data from the their team table in the published S3 bucket
    Then The user is able to read the data

  @fixture.setup.rbac_ucs_opsmi_redacted__all
  Scenario: A UCS OpsMI Redacted team user attempts to access data in another table in the published S3 bucket
    Given A user is not cleared to access other tables than ucs_opsmi_redacted table in the published S3 bucket
    When The user attempts to read data from another table in the published S3 bucket
    Then The user is unable to read the data

  @fixture.setup.rbac_ucs_opsmi_redacted__all
  Scenario: A UCS OpsMI Redacted team user attempts to access PII data from the UC database in the published S3 bucket
    Given A user is not cleared to access PII data from the UC database in the published S3 bucket
    When The user Attempts to read data tagged with the pii:true tag from the UC database in the published S3 bucket
    Then The user is unable to read the data

  @fixture.setup.rbac_ucs_opsmi_redacted__all
  @fixture.cleanup.role_and_s3
  Scenario: A UCS OpsMI Redacted team user attempts to access non PII data from the UC database in the published S3 bucket
    Given A user is not cleared to access non PII data from the UC database in the published S3 bucket
    When The user Attempts to read data tagged with the pii:false tag from the UC database in the published S3 bucket
    Then The user is unable to read the data

  @fixture.setup.rbac_ucs_opsmi_unredacted__all_and_role
  Scenario: A UCS OpsMI Unredacted team user attempts to access data in the ucs_opsmi_redacted table in the published S3 bucket
    Given A user is only cleared to access ucs_opsmi_unredacted table in the published S3 bucket
    When The user attempts to read data from the their team table in the published S3 bucket
    Then The user is able to read the data

  @fixture.setup.rbac_ucs_opsmi_unredacted__all
  Scenario: A UCS OpsMI Unredacted team user attempts to access data in another table in the published S3 bucket
    Given A user is not cleared to access other tables than ucs_opsmi_unredacted table in the published S3 bucket
    When The user attempts to read data from another table in the published S3 bucket
    Then The user is unable to read the data

  @fixture.setup.rbac_ucs_opsmi_unredacted__all
  Scenario: A UCS OpsMI Unredacted team user attempts to access PII data from the UC database in the published S3 bucket
    Given A user is not cleared to access PII data from the UC database in the published S3 bucket
    When The user Attempts to read data tagged with the pii:true tag from the UC database in the published S3 bucket
    Then The user is unable to read the data

  @fixture.setup.rbac_ucs_opsmi_unredacted__all
  @fixture.cleanup.role_and_s3
  Scenario: A UCS OpsMI Unredacted team user attempts to access non PII data from the UC database in the published S3 bucket
    Given A user is not cleared to access non PII data from the UC database in the published S3 bucket
    When The user Attempts to read data tagged with the pii:false tag from the UC database in the published S3 bucket
    Then The user is unable to read the data

  @fixture.setup.rbac_uc_ris_redacted__all_and_role
  Scenario: A UC RIS Redacted team user attempts to access data in the uc_ris_redacted table in the published S3 bucket
    Given A user is only cleared to access uc_ris_redacted table in the published S3 bucket
    When The user attempts to read data from the their team table in the published S3 bucket
    Then The user is able to read the data

  @fixture.setup.rbac_uc_ris_redacted__all
  Scenario: A UC RIS Redacted team user attempts to access data in another table in the published S3 bucket
    Given A user is not cleared to access other tables than uc_ris_redacted table in the published S3 bucket
    When The user attempts to read data from another table in the published S3 bucket
    Then The user is unable to read the data

  @fixture.setup.rbac_uc_ris_redacted__all
  Scenario: A UC RIS Redacted team user attempts to access PII data from the UC database in the published S3 bucket
    Given A user is not cleared to access PII data from the UC database in the published S3 bucket
    When The user Attempts to read data tagged with the pii:true tag from the UC database in the published S3 bucket
    Then The user is unable to read the data

  @fixture.setup.rbac_uc_ris_redacted__all
  @fixture.cleanup.role_and_s3
  Scenario: A UC RIS Redacted team user attempts to access non PII data from the UC database in the published S3 bucket
    Given A user is not cleared to access non PII data from the UC database in the published S3 bucket
    When The user Attempts to read data tagged with the pii:false tag from the UC database in the published S3 bucket
    Then The user is unable to read the data

  @fixture.setup.rbac_uc_ris_unredacted__all_and_role
  Scenario: A UC RIS Redacted team user attempts to access data in the uc_ris_redacted table in the published S3 bucket
    Given A user is only cleared to access uc_ris_unredacted table in the published S3 bucket
    When The user attempts to read data from the their team table in the published S3 bucket
    Then The user is able to read the data

  @fixture.setup.rbac_uc_ris_unredacted__all
  Scenario: A UC RIS Redacted team user attempts to access data in another table in the published S3 bucket
    Given A user is not cleared to access other tables than uc_ris_unredacted table in the published S3 bucket
    When The user attempts to read data from another table in the published S3 bucket
    Then The user is unable to read the data

  @fixture.setup.rbac_uc_ris_unredacted__all
  Scenario: A UC RIS Unredacted team user attempts to access PII data from the UC database in the published S3 bucket
    Given A user is not cleared to access PII data from the UC database in the published S3 bucket
    When The user Attempts to read data tagged with the pii:true tag from the UC database in the published S3 bucket
    Then The user is unable to read the data

  @fixture.setup.rbac_uc_ris_unredacted__all
  @fixture.cleanup.role_and_s3
  Scenario: A UC RIS Unredacted team user attempts to access non PII data from the UC database in the published S3 bucket
    Given A user is not cleared to access non PII data from the UC database in the published S3 bucket
    When The user Attempts to read data tagged with the pii:false tag from the UC database in the published S3 bucket
    Then The user is unable to read the data

  @fixture.setup.rbac_uc__auditlog_redacted_and_sc_and_role
  Scenario: A user with auditlog redacted and SC access attempts to access data in the auditlog_red_v table in the published S3 bucket
    Given A user is cleared to access data in the auditlog_red_v table in the published S3 bucket
    When The user attempts to read data from the auditlog_red_v table in the published S3 bucket
    Then The user is able to read the data

  @fixture.setup.rbac_uc__auditlog_redacted_and_sc
  Scenario: A user with auditlog redacted and SC access attempts to access PII data in the published S3 bucket
    Given A user is cleared to access PII data in the published S3 bucket
    When The user attempts to read data tagged with the pii:true tag in the published S3 bucket
    Then The user is able to read the data

  @fixture.setup.rbac_uc__auditlog_redacted_and_sc
  Scenario: A user with auditlog redacted and SC access attempts to access data in the auditlog_unred_v table in the published S3 bucket
    Given A user is not cleared to access data in the auditlog_unred_v table in the published S3 bucket
    When The user attempts to read data from the auditlog_unred_v table in the published S3 bucket
    Then The user is unable to read the data

  @fixture.setup.rbac_uc__auditlog_redacted_and_sc
  @fixture.cleanup.role_and_s3
  Scenario: A user with auditlog redacted and SC access attempts to access data in the auditlog_sec_v table in the published S3 bucket
    Given A user is not cleared to access data in the auditlog_sec_v table in the published S3 bucket
    When The user attempts to read data from the auditlog_sec_v table in the published S3 bucket
    Then The user is unable to read the data

  @fixture.setup.rbac_uc__auditlog_redacted_and_non_sc_and_role
  Scenario: A user with auditlog redacted and Non SC access attempts to access data in the auditlog_red_v table in the published S3 bucket
    Given A user is cleared to access data in the auditlog_red_v table in the published S3 bucket
    When The user attempts to read data from the auditlog_red_v table in the published S3 bucket
    Then The user is able to read the data

  @fixture.setup.rbac_uc__auditlog_redacted_and_non_sc
  Scenario: A user with auditlog redacted and Non SC access attempts to access non PII data in the published S3 bucket
    Given A user is cleared to access non PII data in the published S3 bucket
    When The user attempts to read data tagged with the pii:false tag in the published S3 bucket
    Then The user is able to read the data

  @fixture.setup.rbac_uc__auditlog_redacted_and_non_sc
  Scenario: A user with auditlog redacted and Non SC access attempts to access non PII data in the published S3 bucket
    Given A user is not cleared to access PII data in the published S3 bucket
    When The user attempts to read data tagged with the pii:true tag in the published S3 bucket
    Then The user is unable to read the data

  @fixture.setup.rbac_uc__auditlog_redacted_and_non_sc
  Scenario: A user with auditlog redacted and Non SC access attempts to access data in the auditlog_unred_v table in the published S3 bucket
    Given A user is not cleared to access data in the auditlog_unred_v table in the published S3 bucket
    When The user attempts to read data from the auditlog_unred_v table in the published S3 bucket
    Then The user is unable to read the data

  @fixture.setup.rbac_uc__auditlog_redacted_and_non_sc
  @fixture.cleanup.role_and_s3
  Scenario: A user with auditlog redacted and Non SC access attempts to access data in the auditlog_sec_v table in the published S3 bucket
    Given A user is not cleared to access data in the auditlog_sec_v table in the published S3 bucket
    When The user attempts to read data from the auditlog_sec_v table in the published S3 bucket
    Then The user is unable to read the data

  @fixture.setup.rbac_uc__auditlog_redacted_and_auditlog_secure_and_role
  Scenario: A user with auditlog redacted and auditlog secure access attempts to access data in the auditlog_red_v table in the published S3 bucket
    Given A user is cleared to access data in the auditlog_red_v table in the published S3 bucket
    When The user attempts to read data from the auditlog_red_v table in the published S3 bucket
    Then The user is able to read the data

  @fixture.setup.rbac_uc__auditlog_redacted_and_auditlog_secure
  Scenario: A user with auditlog redacted and auditlog secure access data in the auditlog_sec_v table in the published S3 bucket
    Given A user is cleared to access data in the auditlog_sec_v table in the published S3 bucket
    When The user attempts to read data from the auditlog_sec_v table in the published S3 bucket
    Then The user is able to read the data

  @fixture.setup.rbac_uc__auditlog_redacted_and_auditlog_secure
  @fixture.cleanup.role_and_s3
  Scenario: A user with auditlog redacted and auditlog secure access data in the auditlog_unred_v table in the published S3 bucket
    Given A user is not cleared to access data in the auditlog_unred_v table in the published S3 bucket
    When The user attempts to read data from the auditlog_unred_v table in the published S3 bucket
    Then The user is unable to read the data

  @fixture.setup.rbac_uc__sc_and_non_sc_and_role
  Scenario: A user with SC and Non SC access attempts to access non PII data in the published S3 bucket
    Given A user is cleared to access non PII data in the published S3 bucket
    When The user attempts to read data tagged with the pii:false tag in the published S3 bucket
    Then The user is able to read the data

  @fixture.setup.rbac_uc__sc_and_non_sc
  Scenario: A user with SC and Non SC access attempts to access PII data in the published S3 bucket
    Given A user is cleared to access PII data in the published S3 bucket
    When The user attempts to read data tagged with the pii:true tag in the published S3 bucket
    Then The user is able to read the data

  @fixture.setup.rbac_uc__sc_and_non_sc
  Scenario: A user with SC and Non SC access attempts to access data in the auditlog_red_v table in the published S3 bucket
    Given A user is not cleared to access data in the auditlog_red_v table in the published S3 bucket
    When The user attempts to read data from the auditlog_red_v table in the published S3 bucket
    Then The user is unable to read the data

  @fixture.setup.rbac_uc__sc_and_non_sc
  Scenario: A user with SC and Non SC access attempts to access data in the auditlog_unred_v table in the published S3 bucket
    Given A user is not cleared to access data in the auditlog_unred_v table in the published S3 bucket
    When The user attempts to read data from the auditlog_unred_v table in the published S3 bucket
    Then The user is unable to read the data

  @fixture.setup.rbac_uc__sc_and_non_sc
  @fixture.cleanup.role_and_s3
  Scenario: A user with SC and Non SC access attempts to access data in the auditlog_sec_v table in the published S3 bucket
    Given A user is not cleared to access data in the auditlog_sec_v table in the published S3 bucket
    When The user attempts to read data from the auditlog_sec_v table in the published S3 bucket
    Then The user is unable to read the data
