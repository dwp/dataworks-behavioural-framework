from behave import given, when, then
import time
import os
from datetime import datetime

from helpers import (
    aws_helper,
    data_ingress_helper,
    console_printer
)


ASG = 'data-ingress-ag'
CLUSTER = 'data-ingress'
FILENAME = 'BasicCompanyData-'
S3_PREFIX = 'data-ingress/companies'
PASS_FILE_KEY = "e2e/eicar_test/pass_"
TIMEOUT = 300


@given("two instances are available for placing tasks in the ecs cluster")
def step_impl(context):
    data_ingress_helper.set_asg_instance_count(ASG, 0, 2, 2)
    data_ingress_helper.check_instance_count(desired_count=2, max_wait=3*60)
    console_printer.print_info("scaling successful")


@then("run sender agent task to send test data and receiver agent task")
def step_impl(context):
    data_ingress_helper.restart_service("sft_agent_sender", CLUSTER)
    data_ingress_helper.restart_service("sft_agent_receiver", CLUSTER)


@then("check if the test file is in s3")
def step_impl(context):
    td = datetime.today().strftime('%Y-%m-%d')
    filename = FILENAME+td+'.csv'
    console_printer.print_info(f"checking if file {filename} is present on s3 bucket")
    start = time.time()
    while not aws_helper.does_s3_key_exist(context.data_ingress_stage_bucket, os.path.join(S3_PREFIX, filename)):
        if time.time()-start < TIMEOUT:
            time.sleep(5)
        else:
            raise AssertionError(f"sft file was not sent and received after {TIMEOUT} seconds")


@then("Wait for pass file indicating that test virus file was correctly detected")
def step_wait_pass_file(context):
    start = time.time()
    while not aws_helper.does_s3_key_exist(context.data_ingress_stage_bucket, PASS_FILE_KEY):
        if time.time()-start < TIMEOUT:
            time.sleep(5)
        else:
            raise AssertionError(f"eicar test did not pass after {TIMEOUT} seconds")


@then("reset the desired and max instance count in the asg to 0")
def step_impl(context):
    data_ingress_helper.set_asg_instance_count(ASG, 0, 0, 0)
    data_ingress_helper.check_instance_count(desired_count=0, max_wait=3*60)
    console_printer.print_info("scaling successful")


@when("data-ingress service restarts")
def step_restart_service(context):
    console_printer.print_info(f"Restarting sft task")
    data_ingress_helper.restart_service("data-ingress", "data-ingress")


