from datetime import datetime, timedelta
import time
from helpers import (
    aws_helper,
    console_printer,
    data_ingress_helper
)


def run_sft_tasks(tasks, cluster):

    for i in tasks:
        client = aws_helper.get_client("ecs")
        response = client.run_task(
            cluster=cluster,
            taskDefinition=i,
        )


def check_container_instance_count(cluster, desired_count, max_wait=540):
    t0 = time.time()
    t1 = t0 + max_wait
    ic = "unknown"
    while time.time() < t1:
        ecs = aws_helper.get_client("ecs")
        response = ecs.list_container_instances(
            cluster=cluster
        )
        ic = len(response['containerInstanceArns'])
        console_printer.print_info(f"instance count: {ic}")
        s = t1 - time.time()
        console_printer.print_info(f"seconds before timeout: {round(s)}")
        if ic == desired_count:
            console_printer.print_info(f"container instances scaled up to {ic} within the time frame given")
            break
        time.sleep(5)
    if ic != desired_count:
        raise AssertionError(f"container instance count: {ic} did not reach desired size: {desired_count} within the time"
                             f" frame given")



def set_asg_instance_count(asg_name, min, max, desired):

    client = aws_helper.get_client("autoscaling")
    response = client.put_scheduled_update_group_action(
        ScheduledActionName="sft-e2e",
        StartTime=datetime.today() + timedelta(hours=0, minutes=1),
        AutoScalingGroupName=asg_name,
        MinSize=min,
        MaxSize=max,
        DesiredCapacity=desired,
    )


def check_instance_count(desired_count, max_wait=240):
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
        raise AssertionError(f"instance count: {ic} did not reach desired size: {desired_count} within the time"
                             f" frame given")


def check_task_state(cluster, family, desired_status):
    ecs = aws_helper.get_client('ecs')
    tasks = ecs.list_tasks(cluster=cluster, desiredStatus=desired_status, family=family)
    try:
        return len(tasks['taskArns']) >= 1
    except:
        console_printer.print_error_text(f"no {family} tasks with status {desired_status} found in cluster:")
        return False
