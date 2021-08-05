import json
from helpers import aws_helper


def invoke_hbase_retriever(payload):
    """Retrieves given data from HBase and returns the payload.

    Keyword arguments:
    payload -- the input for the lamdba invocation
    """
    return aws_helper.invoke_lambda_function(
        "stub_ucfs_hbase_retriever", payload
    ).decode()


def invoke_asg_resizer(payload):
    """Triggers the HTME or Snapshot sender processes:
    Calls the lambda which scales the auto-scaling group which in turn starts the process.

    Keyword arguments:
    payload -- the input for the lamdba invocation
    """
    return aws_helper.invoke_lambda_function("asg_resizer", payload).decode()


def invoke_rbac_test(payload):
    """Triggers the RBAC test lambda:
    Calls the lambda which attempts to access PII data in the EMR cluster

    Keyword arguments:
    payload -- the input for the lamdba invocation
    """
    response = aws_helper.invoke_lambda_function(
        "aws-analytical-env-rbac-test", payload
    )
    return json.loads(response.decode("utf-8"))


def invoke_ingestion_metadata_query_lambda(payload):
    """Runs a query against the metadata store and returns results as a dict.

    Keyword arguments:
    payload -- the input for the lamdba invocation
    """
    response = aws_helper.invoke_lambda_function("ingestion-metadata-query", payload)
    return json.loads(response.decode())


def invoke_adg_emr_launcher_lambda(payload):
    """Triggers adg_emr_launcher lambda with the given payload.

    Keyword arguments:
    payload -- the input for the lambda invocation
    """
    response = aws_helper.invoke_lambda_function("adg_emr_launcher", payload)
    return json.loads(response.decode())


def invoke_clive_emr_launcher_lambda(payload):
    """Triggers aws_clive_emr_launcher lambda with the given payload.

    Keyword arguments:
    payload -- the input for the lambda invocation
    """
    response = aws_helper.invoke_lambda_function("aws_clive_emr_launcher", payload)
    return json.loads(response.decode())


def invoke_uc_feature_emr_launcher_lambda(payload):
    """Triggers uc_feature_emr_launcher lambda with the given payload.

    Keyword arguments:
    payload -- the input for the lambda invocation
    """
    response = aws_helper.invoke_lambda_function("uc_feature_emr_launcher", payload)
    return json.loads(response.decode())


def invoke_pdm_emr_launcher_lambda(payload):
    """Triggers pdm_emr_launcher lambda with the given payload.

    Keyword arguments:
    payload -- the input for the lambda invocation
    """
    response = aws_helper.invoke_lambda_function("pdm_emr_launcher", payload)
    return json.loads(response.decode())


def invoke_intraday_emr_launcher_lambda(payload):
    """Triggers incremental_ingest_replica_emr_launcher lambda with the given payload.

    Keyword arguments:
    payload -- the input for the lambda invocation
    """
    response = aws_helper.invoke_lambda_function("intraday-emr-launcher", payload)
    return json.loads(response.decode())


def invoke_claimant_mysql_metadata_interface(payload=None):
    """Triggers invoke_claimant_mysql_metadata_interface lambda with the given payload.

    Keyword arguments:
    lamdba_name -- the name of the lambda function
    payload -- the input for the lambda invocation (can be None)
    """
    response = aws_helper.invoke_lambda_function("ucfs_claimant_mysql_interface")
    return json.loads(response.decode())


def invoke_kickstart_adg_emr_launcher_lambda(payload):
    """Triggers kickstart_adg_emr_launcher lambda with the given payload.

    Keyword arguments:
    payload -- the input for the lambda invocation
    """
    response = aws_helper.invoke_lambda_function("kickstart_adg_emr_launcher", payload)
    return json.loads(response.decode())


def invoke_mongo_latest_emr_launcher_lambda(payload):
    """Triggers aws_mongo_latest_emr_launcher lambda with the given payload.

    Keyword arguments:
    payload -- the input for the lambda invocation
    """
    response = aws_helper.invoke_lambda_function("mongo_latest_emr_launcher", payload)
    return json.loads(response.decode())
