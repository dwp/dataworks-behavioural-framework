import time
from helpers import aws_helper, console_printer


def dataworks_init_kafka_producer(context):
    console_printer.print_info("Initialising e2e tests...")
    queue_name = context.dataworks_model_sqs_queue

    # Get instance id
    instance_id = aws_helper.get_instance_id("dataworks-kafka-producer")

    # Purge sqs queue
    aws_helper.purge_sqs_queue(queue_name=queue_name)

    # Execute the shell script - stop any e2e test app
    console_printer.print_info(
        f"Initialising e2e tests...stop any old e2e tests running on {context.dataworks_kafka_producer_instance}"
    )
    linux_command = "sh /home/ec2-user/kafka/utils/stop_e2e_tests.sh"
    aws_helper.execute_linux_command(
        instance_id=instance_id,
        linux_command=linux_command,
    )

    # Execute the shell script - delete e2e test topic if it exists
    console_printer.print_info("Initialising e2e tests...remove any e2e test topics")
    linux_command = (
        "sh /home/ec2-user/kafka/utils/run_delete_topic.sh e2e_flagged_journals"
    )
    aws_helper.execute_linux_command(
        instance_id=instance_id,
        linux_command=linux_command,
    )

    # Create a topic for e2e tests
    console_printer.print_info(
        "Initialising e2e tests...create e2e_flagged_journals topic"
    )
    linux_command = (
        "sh /home/ec2-user/kafka/utils/run_create_topic.sh e2e_flagged_journals"
    )
    aws_helper.execute_linux_command(
        instance_id=instance_id,
        linux_command=linux_command,
    )

    # wait for a 60secs
    time.sleep(int(60))
    console_printer.print_info("COMPLETE:Initialising e2e tests...")


def dataworks_stop_kafka_producer_app(context):
    console_printer.print_info("Executing 'stop_kafka_producer_app' fixture")

    # Get instance id
    instance_id = aws_helper.get_instance_id("dataworks-kafka-producer")

    # Execute the shell script - stop the e2e test application
    linux_command = "sh /home/ec2-user/kafka/utils/stop_e2e_tests.sh"
    aws_helper.execute_linux_command(
        instance_id=instance_id,
        linux_command=linux_command,
    )
    console_printer.print_info("Executing 'stop_kafka_producer_app' fixture....complete")
