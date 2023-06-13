from behave import given, then, when
from datetime import datetime
import os
import time
from helpers import aws_helper, console_printer


ASG = "data-ingress-ag"
CLUSTER = "data-ingress"
FILENAME = "BasicCompanyData-"
S3_PREFIX = "e2e/data-ingress/companies"
TEST_FILE_KEY = "e2e/eicar_test/pass.txt"
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


@given("ASG instances are running")
def step_impl(context):
    aws_helper.check_instance_count(desired_count=2, asg_name="data-ingress-ag")
    console_printer.print_info("data-ingress-ag is running")


@given("ECS cluster has instances attached")
def step_impl(context):
    aws_helper.check_container_instance_count(CLUSTER, 2)
    console_printer.print_info("waiting for container instances to be available")


@given("sender agent task and receiver agent task are running")
def step_impl(context):
    start = time.time()
    receiver_running = aws_helper.check_task_state(
        CLUSTER, family="sft_agent_receiver", desired_status="running"
    )
    sender_running = aws_helper.check_task_state(
        CLUSTER, family="sft_agent_sender", desired_status="running"
    )

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


@when("the test file is submitted to the sender SFT agent")
def step_impl(context):
    # remove all sft files currently in the stub nifi output bucket
    aws_helper.clear_s3_prefix(
        context.data_ingress_stage_bucket, S3_PREFIX, False
    )

    console_printer.print_info(f"Executing commands on Ec2")
    commands = [
        "sudo su",
        f"cd /var/lib/docker/volumes/data-egress/_data/{file_location}",
        f"echo \"ab,c,de\" >> /mnt/send_point/prod217.csv",
    ]
    aws_helper.execute_commands_on_ec2_by_tags_and_wait(
        commands, ["dataworks-aws-data-egress"], 30
    )

@then("new test file sent by sft sender is on s3")
def step_impl(context):
    td = datetime.today().strftime("%Y-%m-%d")
    filename = FILENAME + td + ".zip"
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
