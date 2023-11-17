@workflow-orchestration-poc
Feature: Workflow Orchestration Service handles Events from EventBridge

  Background: We have infrastructure setup for Workflow Orchestration Event Listener
    Given an Event Bus named 'workflow_orchestration_bus' exists
    And a Lambda Function named 'workflow_orchestrator_event_listener:wo_event_listener' exists
    And an SQS queue named 'workflow_orchestration_task_events.fifo' for testing exists and is purged

  Scenario: The Workflow Orchestration Event Listener receives an EMR Cluster Event
    Given an Event Rule to handle 'EMR Cluster State Change' events that targets the Lambda
    When an Event of type 'EMRClusterEvent' is fired to the Event Bus
    Then the Workflow Orchestration Event Listener receives the event and puts it onto the SNS topic

Feature: Workflow Orchestration Service records Task Events

    Background: We have infrastructure setup for Workflow Orchestration Task Recorder
      Given a Table of type 'dynamodb' named 'workflow_orchestration_db' exists
      And a Lambda Function named 'wokflow_orchestrator_task_recorder' exists
      And an SNS Topic named 'workflow_orchestration_task_events.fifo' exists

    Scenario: The Workflow Orchestration Task Recorder receives a Task Event and records it
      Given an Event of type 'EMRClusterEvent' is placed onto the SNS Topic
      Then we expect a Record of this type in the database

# Feature: Workflow Orchestration Service receives requests from API Gateway

#   Scenario: The Workflow Orchestration Task Submitter receives arguments from API Gateway
#     Given a request to the API Gateway launch a 'emr-launcher' task
#     Then we should expect an EMR cluster
#     And we should receive a task id and a 'emr-launcher' response

