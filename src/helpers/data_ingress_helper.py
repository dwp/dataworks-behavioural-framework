from helpers import aws_helper
import datetime


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
