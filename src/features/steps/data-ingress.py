from behave import given, then, when
from datetime import datetime
import os
import time
from helpers import aws_helper, console_printer


ASG = "data-ingress-ag"
CLUSTER = "data-ingress"
FILENAME = "BasicCompanyData-"
S3_PREFIX = "e2e/data-ingress/companies"
PASS_FILE_KEY = "e2e/eicar_test/pass.txt"
TIMEOUT_TM = 360
TIMEOUT_SFT = 540
TIMEOUT = 600


@given(
    "instances should start in '{time_scale_up}' and stop in '{time_scale_down}' min after the pipeline has run"
)
def step_impl(context, time_scale_up, time_scale_down):
    try:
        context.time_scale_up = int(time_scale_up)
        context.time_scale_down = int(time_scale_down)
        context.time_start = time.time()
    except Exception as ex:
        console_printer.print_error_text(ex)


@when("instance starts within the expected time")
def step_impl(context):

    w = (context.time_scale_up * 60) - 100
    console_printer.print_info(f"waiting {w} seconds")
    time.sleep(w)
    aws_helper.check_instance_count(desired_count=2)
    console_printer.print_info("scaling successful")


@when("sender agent task and receiver agent task run")
def step_impl(context):
    aws_helper.check_container_instance_count(CLUSTER, 2)
    console_printer.print_info("waiting for container instances to be available")
    time.sleep(20)
    start = time.time()
    receiver_running = aws_helper.check_task_state(
        CLUSTER, family="sft_agent_receiver", desired_status="running"
    )
    sender_running = aws_helper.check_task_state(
        CLUSTER, family="sft_agent_sender", desired_status="running"
    )
    aws_helper.run_ecs_task("sft_agent_receiver", CLUSTER)
    time.sleep(20)
    aws_helper.run_ecs_task("sft_agent_sender", CLUSTER)

    while receiver_running == False or sender_running == False:
        if time.time() - start < TIMEOUT:
            time.sleep(20)
            receiver_running = aws_helper.check_task_state(
                CLUSTER, family="sft_agent_receiver", desired_status="running"
            )
            sender_running = aws_helper.check_task_state(
                CLUSTER, family="sft_agent_sender", desired_status="running"
            )
        else:
            raise AssertionError(
                f"couldn't get receiver and sender to running state after {TIMEOUT} seconds"
            )


@then("new trend micro test pass file is on s3")
def step_wait_pass_file(context):
    start = time.time()
    while not aws_helper.check_if_s3_object_exists(
        context.data_ingress_stage_bucket, PASS_FILE_KEY
    ):
        if time.time() - start < TIMEOUT_TM:
            time.sleep(5)
            time_left = time.time() - start
            tl = TIMEOUT_TM - round(time_left)
            console_printer.print_info(f"timeout in {tl} seconds")
        else:
            raise AssertionError(f"eicar test did not pass after {TIMEOUT_TM} seconds")


@then("new test file sent by sft sender is on s3")
def step_impl(context):
    td = datetime.today().strftime("%Y-%m-%d")
    filename = FILENAME + td + ".csv"
    console_printer.print_info(f"checking if file {filename} is present on s3 bucket")
    start = time.time()

    while not aws_helper.check_if_s3_object_exists(
        context.data_ingress_stage_bucket, os.path.join(S3_PREFIX, filename)
    ):
        if time.time() - start < TIMEOUT_SFT:
            time.sleep(5)
        else:
            raise AssertionError(
                f"sft file was not sent and received after {TIMEOUT_SFT} seconds"
            )


@then("instance stops within the expected time")
def step_impl(context):
    time_now = time.time()
    w = (context.time_scale_down * 60) - (time_now - context.time_start)
    if w > 0:
        console_printer.print_info(f"waiting {w} seconds")
        time.sleep(w)
    aws_helper.check_instance_count(desired_count=0, asg_name="data-ingress-ag")
    console_printer.print_info("scaling successful")
