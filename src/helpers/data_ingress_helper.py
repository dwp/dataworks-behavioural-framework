from helpers import aws_helper
import datetime

def check_task_running(cluster, task_name):

    arns = aws_helper.get_task_arns(cluster=cluster)
    arns = [arn for arn in arns if task_name in arn]
    assert len(arns) == 1, "there are multiple tasks "
    responses = aws_helper.describe_tasks(cluster=cluster, arns=arns)
    task_status = responses["tasks"][0]["lastStatus"]
    assert task_status == "RUNNING", "task status is {}"
    aws_helper.console_printer.print_info(f"{task_name} is running")


def restart_service(service, cluster):

    client = aws_helper.get_client("ecs")
    response = client.update_service(cluster=cluster, service=service, forceNewDeployment=True)


def set_asg_instance_count(asg_name, min, max, desired):

    client = aws_helper.get_client("autoscaling")
    response = client.put_scheduled_update_group_action(
        AutoScalingGroupName=asg_name,
        Time=datetime.today(),
        MinSize=min,
        MaxSize=max,
        DesiredCapacity=desired,
        TimeZone='Europe/London'
    )
