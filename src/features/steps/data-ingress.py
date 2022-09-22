from behave import given, then
from datetime import datetime
import os
import time
from helpers import (
    aws_helper,
    console_printer,
    data_ingress_helper
)


ASG = 'data-ingress-ag'
CLUSTER = 'data-ingress'
FILENAME = 'BasicCompanyData-'
S3_PREFIX = 'data-ingress/companies'
PASS_FILE_KEY = "e2e/eicar_test/not_passed.txt"
TIMEOUT = 300


@given("the autoscaling schedules replicas that are set to scale up after '{time_scale_up}' min"
       "and scale down after '{time_scale_down}' min")
def step_impl(context, time_scale_up, time_scale_down):
    try:
        context.time_scale_up = int(time_scale_up)
        context.time_scale_down = int(time_scale_down)
        context.time_start = time.time()
    except Exception as ex:
        console_printer.print_error_text(ex)


@then("wait for the instance to scale up within the expected time")
def step_impl(context):

    w = (context.time_scale_up*60)-100
    console_printer.print_info(f"waiting {w} seconds")
    time.sleep(w)
    data_ingress_helper.check_instance_count(desired_count=2)
    console_printer.print_info("scaling successful")


@then("run sender agent task to send test data and receiver agent task")
def step_impl(context):
    data_ingress_helper.restart_service("sft_agent_receiver", CLUSTER)
    start = time.time()
    receiver_running = data_ingress_helper.check_task_state(CLUSTER, family="sft_agent_receiver", desired_status="running")
    sender_running = data_ingress_helper.check_task_state(CLUSTER, family="sft_agent_sender", desired_status="running")
    while not receiver_running & sender_running:
        if time.time()-start < TIMEOUT:
            time.sleep(15)
            receiver_running = data_ingress_helper.check_task_state(CLUSTER, family="sft_agent_receiver",
                                                                    desired_status="running")
            sender_running = data_ingress_helper.check_task_state(CLUSTER, family="sft_agent_sender",
                                                                  desired_status="running")
            data_ingress_helper.restart_service("sft_agent_sender", CLUSTER)
            data_ingress_helper.restart_service("sft_agent_receiver", CLUSTER)
        else:
            raise AssertionError(f"couldn't get both sender and receiver to running state after {TIMEOUT} seconds")


@then("wait for pass file indicating that test virus file was correctly detected")
def step_wait_pass_file(context):
    start = time.time()
    while not aws_helper.check_if_s3_object_exists(context.data_ingress_stage_bucket, PASS_FILE_KEY):
        if time.time()-start < TIMEOUT:
            time.sleep(5)
            time_left = time.time() - start
            console_printer.print_info(f"timeout in {time_left} seconds")
        else:
            raise AssertionError(f"eicar test did not pass after {TIMEOUT} seconds")


@then("check if the test file is in s3")
def step_impl(context):
    td = datetime.today().strftime('%Y-%m-%d')
    filename = FILENAME+td+'.csv'
    console_printer.print_info(f"checking if file {filename} is present on s3 bucket")
    start = time.time()
    while not aws_helper.check_if_s3_object_exists(context.data_ingress_stage_bucket,os.path.join(S3_PREFIX, filename)):
        if time.time()-start < TIMEOUT:
            time.sleep(5)
        else:
            raise AssertionError(f"sft file was not sent and received after {TIMEOUT} seconds")


@then("wait for the instance to scale down within the expected time")
def step_impl(context):
    time_now = time.time()
    w = (context.time_scale_down * 60) - (time_now - context.time_start)
    if w > 0:
        console_printer.print_info(f"waiting {w} seconds")
        time.sleep(w)
    data_ingress_helper.check_instance_count(desired_count=0)
    console_printer.print_info("scaling successful")
