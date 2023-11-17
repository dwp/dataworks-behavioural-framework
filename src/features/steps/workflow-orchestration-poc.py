import json
from behave import given, when, then
from helpers import aws_helper

@given("an Event Bus named '{event_bus_name}' exists")
def step_impl(context, event_bus_name):
    assert(
        aws_helper.event_bus_exists(event_bus_name=event_bus_name)
    )
    context.workflow_orchestration_event_bus_name = event_bus_name

@given("a Lambda Function named '{function_name}' exists")
def step_impl(context, function_name):
    function_arn = aws_helper.get_function_arn(function_name)
    context.function_arn = function_arn

@given("an SQS queue named '{queue_name}' for testing exists and is purged")
def step_impl(context, queue_name):
    queue_url = aws_helper.get_queue_url(queue_name)
    aws_helper.purge_sqs_queue(queue_name)
    context.queue_url = queue_url

@given("an Event Rule to handle '{event_detail_type}' events that targets the Lambda")
def step_impl(context, event_detail_type):
    event_rule_name = aws_helper.get_event_rule_name_for_event_type(
        event_detail_type, 
        context.workflow_orchestration_event_bus_name
    )
    assert(event_rule_name is not None)
    assert(
        aws_helper.event_rule_has_target(
            context.function_arn,
            event_rule_name,
            context.workflow_orchestration_event_bus_name
        )
    )

@given("a Table of type '{db_table}' named '{table_name}' exists")
def step_impl(context, db_table, table_name)
    pass

@given("an SNS Topic named '{topic_name}' exists")
def step_impl(context, topic_name):
    pass


@when("an Event of type '{event_type}' is fired to the Event Bus")
def step_impl(context, event_type):
    context.event_type = event_type
    context.event_detail = {
        "severity": "INFO",
        "stateChangeReason": "{\"code\":\"USER_REQUEST\",\"message\":\"Terminated by user request\"}",
        "name": "Development Cluster",
        "clusterId": "j-1YONHTCP3YZKC",
        "state": "TERMINATED",
        "message": "Amazon EMR Cluster j-1YONHTCP3YZKC (Development Cluster) has terminated at 2016-12-16 21:00 UTC with a reason of USER_REQUEST."
    }
    event_entry = {
        'Source': 'test.emr',
        'DetailType': 'EMR Cluster State Change',
        'Detail': json.dumps(context.event_detail),
        'EventBusName': context.workflow_orchestration_event_bus_name
    }
    aws_helper.put_events(
        event_entries=[
            event_entry
        ]
    )

@then("the Workflow Orchestration Event Listener receives the event and puts it onto the SNS topic")
def step_impl(context):
    resp = aws_helper.get_message_of_sqs_queue(context.queue_url)
    body = resp["Body"]
    body_json = json.loads(body)
    msg_json = json.loads(body_json["Message"])
    assert(
        msg_json.get("event_type") == context.event_type
    )

@given("an Event of type '{event_type}' is placed on the SNS Topic")
def step_impl(context, event_type):
    aws_helper.publish_message_to_sns()