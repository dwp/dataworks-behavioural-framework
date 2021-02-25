from helpers import (
    aws_helper,
    console_printer,
)


def send_monitoring_alert(
    monitoring_sns_topic_arn,
    status,
    severity,
    notification_type,
    custom_elements=[],
):
    """Sends a monitoring alert to SNS.

    Arguments:
    monitoring_sns_topic_arn -- the SNS topic arn
    status -- the status to send
    severity -- Low, Medium, High or Critical
    notification_type -- Information, Warning or Error
    custom_elements -- an array of (key, value) tuples for custom elements to add to the alert
    """
    console_printer.print_info(
        f"Sending out monitoring message with status of '{status}', "
        + f"severity of '{severity}', notification_type of '{notification_type}' "
        + f"and custom_elements of '{custom_elements}'"
    )

    message = {
        "severity": severity,
        "notification_type": notification_type,
        "slack_username": "DataWorks Automation Framework",
        "title_text": status,
    }

    if len(custom_elements) > 0:
        message["custom_elements"] = [
            {"key": custom_element[0], "value": custom_element[1]}
            for custom_element in custom_elements
        ]

    aws_helper.publish_message_to_sns(message, monitoring_sns_topic_arn)
