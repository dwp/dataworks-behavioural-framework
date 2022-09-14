from datetime import datetime, timedelta
import time
from helpers import (
    aws_helper,
    console_printer,
    data_ingress_helper
)


def restart_service(service, cluster):

    client = aws_helper.get_client("ecs")
    response = client.update_service(cluster=cluster, service=service, forceNewDeployment=True)


def set_asg_instance_count(asg_name, min, max, desired):

    client = aws_helper.get_client("autoscaling")
    response = client.put_scheduled_update_group_action(
        ScheduledActionName="sft-e2e",
        StartTime=datetime.today() - timedelta(hours=0, minutes=58),
        AutoScalingGroupName=asg_name,
        MinSize=min,
        MaxSize=max,
        DesiredCapacity=desired,
    )


def check_instance_count(desired_count, max_wait=120):
    t0 = time.time()
    t1 = t0 + max_wait
    ic = "unknown"
    while time.time() < t1:
        ic = aws_helper.instance_count_by_tag("aws:autoscaling:groupName", "data-ingress-ag")
        console_printer.print_info(f"instance count: {ic}")
        s = t1 - time.time()
        console_printer.print_info(f"seconds before timeout: {round(s)}")
        if ic == desired_count:
            console_printer.print_info(f"instances scaled up to {ic} within the time frame given")
            break
        time.sleep(10)
    if ic != desired_count:
        raise AssertionError(f"instance count: {ic} did not reach desired size: {desired_count} within the time frame given")
