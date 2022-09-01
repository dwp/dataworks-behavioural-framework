from behave import given, when, then
import time
from helpers import (
    aws_helper,
    data_ingress_helper,
    console_printer
)
import datetime

ASG = 'data-ingress-ag'
CLUSTER = 'data-ingress'
FILENAME_PREFIX='BasicCompanyData'

@given("the autoscaling schedules replicas that are set to scale up after 'time_scale_up' min and scale down after 'time_scale_down' min")
def step_impl(context, time_scale_up, time_scale_down):
    context.time_scale_up = time_scale_up
    context.time_scale_down = time_scale_down
    context.max_wait = 4*60


@then("wait for the instance to scale up within the expected time")
def step_impl(context):
    time.sleep(context.time_scale_up*60)
    t0 = time.time()
    t1 = t0 + context.max_wait
    scaled = False
    while time.time() < t1 and scaled == False:
        if aws_helper.instance_count_by_tag("aws:autoscaling:groupName", ASG) == 1:
            scaled = True
            console_printer.print_info("instance count reached desired size within the time frame given")
        time.sleep(5)
    if scaled == False:
        console_printer.print_error_text("instance count did not reached desired size within the time frame given")
        raise AssertionError("step scale up failed")


@then("wait for the instance to scale down within the expected time")
def step_impl(context):
    time.sleep(context.time_scale_down*60)
    t0 = time.time()
    t1 = t0 + context.max_wait
    scaled = False
    while time.time() < t1 and scaled == False:
        if aws_helper.instance_count_by_tag("aws:autoscaling:groupName", ASG) == 0:
            scaled = True
            console_printer.print_info("instance count reached desired size within the time frame given")
        time.sleep(5)
    if scaled == False:
        console_printer.print_error_text("instance count did not reached desired size within the time frame given")
        raise AssertionError("step scale down failed")


@given("an sft agent task that sends the data to a receiver task running on a different instance")
def step_impl(context):

@then("set the desired and max instance count in the asg to 2 and refresh the sender and receiver services")
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
    data_ingress_helper.restart_service("sft_agent_sender", CLUSTER)
    data_ingress_helper.restart_service("sft_agent_receiver", CLUSTER)


@then("check if the test file ends up in s3")
def step_impl(context):
    td = datetime.today().strftime('%Y-%m-%d')
    filename = FILENAME_PREFIX+td
    aws_helper.check_if_s3_object_exists(
            context.published_bucket, context.analytical_test_data_s3_location.get("path")
        )

@then("reset the desired and max instance count in the asg to 2")
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
