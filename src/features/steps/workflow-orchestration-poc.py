import json
from behave import given, when, then
from helpers import aws_helper

@given("an Event Bus named '{event_bus_name}'")
def step_impl(context, event_bus_name):
    assert(
        aws_helper.event_bus_exists(event_bus_name=event_bus_name)
    )
    context.workflow_orchestration_event_bus_name = event_bus_name

@given("an Event Rule to handle '{event_detail_type}' events that targets the Lambda")
def step_impl(context, event_detail_type):
    event_rule_name = aws_helper.get_event_rule_name_for_event_type(
        event_detail_type, 
        context.workflow_orchestration_event_bus_name
    )
    assert(event_rule_name is not None)
    assert(
        aws_helper.event_rule_has_target(
            "arn:aws:lambda:eu-west-2:475593055014:function:workflow_orchestrator_event_listener:wo_event_listener",
            event_rule_name,
            context.workflow_orchestration_event_bus_name
        )
    )
    

@when("an Event is fired to the Event Bus")
def step_impl(context):
    event_detail = {
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
        'Detail': json.dumps(event_detail),
        'EventBusName': context.workflow_orchestration_event_bus_name
    }
    aws_helper.put_events(
        event_entries=[
            event_entry
        ]
    )

@then("the Workflow Orchestration Event Listener receives the event and puts it onto the SNS topic")
def step_impl(context):
    pass