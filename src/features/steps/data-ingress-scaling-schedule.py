from behave import given, then
import time
from helpers import (
    aws_helper,
    console_printer,
    data_ingress_helper
)


@given("the autoscaling schedules replicas that are set to scale up after '{time_scale_up}' min and scale down after '{time_scale_down}' min")
def step_impl(context, time_scale_up, time_scale_down):
    try:
        context.time_scale_up = int(time_scale_up)
        context.time_scale_down = int(time_scale_down)
    except Exception as ex:
        console_printer.print_error_text(ex)


@then("wait for the instance to scale up within the expected time")
def step_impl(context):

    w = context.time_scale_up*60
    console_printer.print_info(f"waiting {w} seconds")
    time.sleep(w)
    data_ingress_helper.check_instance_count(desired_count=2)
    console_printer.print_info("scaling successful")


@then("wait for the instance to scale down within the expected time")
def step_impl(context):
    w = context.time_scale_down*60 - context.time_scale_up*60
    console_printer.print_info(f"waiting {w} seconds")
    time.sleep(w)
    data_ingress_helper.check_instance_count(desired_count=0)
    console_printer.print_info("scaling successful")
