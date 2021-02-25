import uuid
import time

from behave import when, then, given
from helpers import (
    aws_helper,
    console_printer,
    streaming_data_helper,
)


@given("The '{message_type}' topic override is used for this test")
def step_impl(context, message_type):
    context.topics_for_test = streaming_data_helper.generate_topics_override(
        message_type, context.topics_for_test
    )


@given("The topics are configured with unique names")
def step_impl(context):
    context.topics_for_test = []
    for topic in context.topics_unique:
        context.topics_for_test.append({"topic": topic, "key": str(uuid.uuid4())})


@then("The asg has scaled correctly")
def step_impl(context):
    asg_prefix = context.last_scaled_asg[0]
    desired_count = context.last_scaled_asg[1]

    console_printer.print_info(
        f"Waiting for autoscaling group with prefix of '{asg_prefix}' to set desired count to '{str(desired_count)}'"
    )

    time_taken = 1
    timeout_time = time.time() + context.timeout
    asg_finished_scaling = False

    while not asg_finished_scaling and time.time() < timeout_time:
        count = 0
        while not asg_finished_scaling and count < 60:
            if aws_helper.get_asg_desired_count(None, asg_prefix) == desired_count:
                asg_finished_scaling = True
                break

            time.sleep(5)
            count += 5

        if asg_finished_scaling:
            console_printer.print_info(
                f"Autoscaling group with prefix of '{asg_prefix}' correctly set desired count of '{str(desired_count)}'"
            )
            break
        else:
            console_printer.print_info(
                f"Autoscaling group with prefix of '{asg_prefix}' not set desired count of '{str(desired_count)}' after one minute, so trying to scale again"
            )
            aws_helper.scale_asg_if_desired_count_is_not_already_set(
                asg_prefix, desired_count
            )

    assert (
        asg_finished_scaling
    ), f"Autoscaling group with prefix of '{asg_prefix}' not set desired count to '{str(desired_count)}' after '{str(context.timeout)}' seconds"
