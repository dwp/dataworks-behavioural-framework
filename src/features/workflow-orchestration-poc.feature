@workflow-orchestration-poc
Feature: Workflow Orchestration Service handles Events from EventBridge

  Background: We have infrastructure setup for Workflow Orchestration
    Given an Event Bus named 'workflow_orchestration_bus'

  Scenario: The Workflow Orchestration Event Listener receives an EMR Cluster Event
    Given an Event Rule to handle 'EMR Cluster State Change' events that targets the Lambda
    When an Event is fired to the Event Bus
    Then the Workflow Orchestration Event Listener receives the event and puts it onto the SNS topic

# Feature: Workflow Orchestration Service receives requests from API Gateway

#   Scenario: The Workflow Orchestration Task Submitter receives arguments from API Gateway
#     Given a request to the API Gateway launch a 'emr-launcher' task
#     Then we should expect an EMR cluster
#     And we should receive a task id and a 'emr-launcher' response

