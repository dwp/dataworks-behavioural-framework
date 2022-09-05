from behave import given, when, then
import time
import os
from helpers import (
    aws_helper,
    data_ingress_helper,
    console_printer
)

from datetime import datetime

ASG = 'data-ingress-ag'
CLUSTER = 'data-ingress'
FILENAME = 'BasicCompanyData-'
S3_PREFIX = 'data-ingress/companies'


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
    time.sleep(5*60)
    td = datetime.today().strftime('%Y-%m-%d')
    filename = FILENAME+td+'.csv'
    console_printer.print_info(f"checking if file {filename} is present on s3 bucket")
    if not aws_helper.check_if_s3_object_exists(context.data_ingress_stage_bucket, os.path.join(S3_PREFIX, filename)):
        raise AssertionError("test file not present in s3")


@then("reset the desired and max instance count in the asg to 0")
def step_impl(context):
    data_ingress_helper.set_asg_instance_count(ASG, 0, 0, 0)
    data_ingress_helper.check_instance_count(desired_count=0, max_wait=3*60)
    console_printer.print_info("scaling successful")
