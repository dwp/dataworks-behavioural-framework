from behave import given, when, then
import time
import os
from helpers import (
    aws_helper,
    data_ingress_helper,
    console_printer
)
import datetime

ASG = 'data-ingress-ag'
CLUSTER = 'data-ingress'
FILENAME = 'TestBasicCompanyData'
S3_PREFIX = 'data-ingress/companies'


@given("two instances are available for placing tasks in the ecs cluster")
def step_impl(context):
    t0 = time.time()
    t1 = t0 + context.max_wait
    data_ingress_helper.set_asg_instance_count(ASG, 0, 2, 2)
    while time.time() < t1 and scaled == False:
        if aws_helper.instance_count_by_tag("aws:autoscaling:groupName", ASG) == 2:
            scaled = True
            console_printer.print_info("instance count reached desired size within the time frame given")
        time.sleep(5)
    if scaled == False:
        console_printer.print_error_text("instance count did not reached desired size within the time frame given")
        raise AssertionError("step scale down failed")


@then("run sender agent task to send test data and receiver agent task")
def step_impl(context):
    data_ingress_helper.restart_service("sft_agent_sender", CLUSTER)
    data_ingress_helper.restart_service("sft_agent_receiver", CLUSTER)


@then("check if the test file in s3")
def step_impl(context):
    time.sleep(5*60)
    td = datetime.today().strftime('%Y-%m-%d')
    filename = FILENAME+td+'.csv'
    # aws_helper.check_if_s3_object_exists(context.data_ingress_stage_bucket, context.analytical_test_data_s3_location.get("path"))
    if not aws_helper.check_if_s3_object_exists("", os.path.join(S3_PREFIX, filename)):
        raise AssertionError("step scale down failed")


@then("reset the desired and max instance count in the asg to 0")
def step_impl(context):
    t0 = time.time()
    t1 = t0 + context.max_wait
    data_ingress_helper.set_asg_instance_count(ASG, 0, 0, 0)
    while time.time() < t1 and scaled == False:
        if aws_helper.instance_count_by_tag("aws:autoscaling:groupName", ASG) == 0:
            scaled = True
            console_printer.print_info("instance count reached desired size within the time frame given")
        time.sleep(5)
    if scaled == False:
        console_printer.print_error_text("instance count did not reached desired size within the time frame given")
        raise AssertionError("step scale down failed")
