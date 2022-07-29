from behave import given, then, when
import os
import zlib
import time
import base64
from helpers import (
    historic_data_load_generator,
    aws_helper,
    console_printer,
    data_ingress_helper,
)

PASS_FILE_KEY = "e2e/eicar_test/pass_"
TIMEOUT = 300


@when("data-ingress service restarts")
def step_restart_service(context):
    console_printer.print_info(f"Restarting sft task")
    data_ingress_helper.restart_service("data-ingress", "data-ingress")


@then("Wait for pass file indicating that test virus file was correctly detected")
def step_wait_pass_file(context):

    start = time.time()
    while not aws_helper.does_s3_key_exist(
        context.data_ingress_stage_bucket, PASS_FILE_KEY
    ):
        if time.time() - start < TIMEOUT:
            time.sleep(5)
        else:
            raise AssertionError(f"eicar test did not pass after {TIMEOUT} seconds")
