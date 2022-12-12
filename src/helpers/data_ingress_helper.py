import time
from helpers import aws_helper, console_printer, data_ingress_helper


def check_container_instance_count(cluster, desired_count, max_wait=600):
    t0 = time.time()
    t1 = t0 + max_wait
    ic = "unknown"
    ecs = aws_helper.get_client("ecs")
    while time.time() < t1:
        response = ecs.list_container_instances(cluster=cluster)
        ic = len(response["containerInstanceArns"])
        console_printer.print_info(f"instance count: {ic}")
        s = t1 - time.time()
        console_printer.print_info(f"seconds before timeout: {round(s)}")
        if ic == desired_count:
            console_printer.print_info(
                f"container instances scaled up to {ic} within the time frame given"
            )
            break
        time.sleep(5)
    if ic != desired_count:
        raise AssertionError(
            f"container instance count: {ic} did not reach desired size: {desired_count} within the time"
            f" frame given"
        )


def check_instance_count(desired_count, max_wait=300):
    t0 = time.time()
    t1 = t0 + max_wait
    ic = "unknown"
    while time.time() < t1:
        ic = aws_helper.instance_count_by_tag(
            "aws:autoscaling:groupName", "data-ingress-ag"
        )
        console_printer.print_info(f"instance count: {ic}")
        s = t1 - time.time()
        console_printer.print_info(f"seconds before timeout: {round(s)}")
        if ic == desired_count:
            console_printer.print_info(
                f"instances scaled up to {ic} within the time frame given"
            )
            break
        time.sleep(10)
    if ic != desired_count:
        raise AssertionError(
            f"instance count: {ic} did not reach desired size: {desired_count} within the time"
            f" frame given"
        )


def check_task_state(cluster, family, desired_status):
    ecs = aws_helper.get_client("ecs")
    tasks = ecs.list_tasks(cluster=cluster, desiredStatus=desired_status, family=family)
    try:
        if len(tasks["taskArns"]) == 1:
            return True
        else:
            return False
    except:
        console_printer.print_error_text(
            f"no {family} tasks with status {desired_status} found in cluster"
        )
