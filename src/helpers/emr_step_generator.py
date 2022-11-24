import os
from helpers import aws_helper, console_printer


def generate_emrfs_step(
    emr_cluster_id,
    emr_cluster_s3_bucket_id,
    emr_cluster_s3_prefix,
    step_type,
    arguments,
):
    """Starts a step of type emrfs and returns its id.

    Keyword arguments:
    emr_cluster_id -- the id of the cluster
    emr_cluster_s3_bucket_id -- the bucket name for the root directory for the EMR cluster
    emr_cluster_s3_prefix -- the prefix for the root directory for the EMR cluster
    step_type -- the step type - either 'sync', 'delete' or 'import'
    arguments -- the arguments to pass to the script as a string, if any
    """

    cluster_sync_root = "s3://" + os.path.join(
        emr_cluster_s3_bucket_id,
        emr_cluster_s3_prefix,
    )

    console_printer.print_info(
        f"Executing emrfs step type '{step_type}' with sync root of \"{cluster_sync_root}\""
    )

    arguments_array = ["emrfs", step_type, f'"{cluster_sync_root}"']

    if arguments:
        arguments_array.extend(arguments.split())

    bash_command = " ".join(arguments_array)

    console_printer.print_info(
        f"Converted emrfs command to bash command of '{bash_command}'"
    )

    step_name = f"Automated EMRFS Step - {step_type}"
    return generate_local_step(emr_cluster_id, bash_command, step_name)


def generate_script_step(
    emr_cluster_id, script_location, step_type, command_line_arguments=None
):
    """Starts a step of type script and returns its id.

    Keyword arguments:
    emr_cluster_id -- the id of the cluster
    script_location -- the location on the EMR instance (or an S3 URI) of the script to run
    step_type -- the name of the step type being run (i.e. "major compaction")
    command_line_arguments -- the arguments to pass to the script as a string, if any
    """
    arguments_array = [script_location]

    if command_line_arguments is not None:
        arguments_array.extend(command_line_arguments.split())

    console_printer.print_info(
        f"Executing script step type '{step_type}' with arguments of \"{arguments_array}\""
    )

    bash_command = " ".join(arguments_array)

    console_printer.print_info(
        f"Converted arguments array to bash command of '{bash_command}'"
    )

    step_name = f"Automated Script Step - {step_type}"
    return generate_local_step(emr_cluster_id, bash_command, step_name)


def generate_spark_step(
    emr_cluster_id,
    script_location,
    step_type,
    extra_python_files=None,
    command_line_arguments=None,
):
    """Starts a step of type script and returns its id.

    Keyword arguments:
    emr_cluster_id -- the id of the cluster
    script_location -- the location on the EMR instance (or an S3 URI) of the script to run
    step_type -- the name of the step type being run (i.e. "major compaction")
    extra_python_files -- comma-delimited list of extra python files to add to the step
    command_line_arguments -- the arguments to pass to the script as a string, if any
    """
    arguments_array = []
    if extra_python_files is not None:
        arguments_array.extend(["--py-files", extra_python_files])

    arguments_array.append(script_location)

    if command_line_arguments is not None:
        arguments_array.extend(command_line_arguments.split())

    console_printer.print_info(
        f"Executing script step type '{step_type}' with arguments of \"{arguments_array}\""
    )

    step_name = f"Automated Script Step - {step_type}"

    step_flow = {
        "Name": step_name,
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--master",
                "yarn",
                "--conf",
                "spark.yarn.submit.waitAppCompletion=true",
            ]
            + arguments_array,
        },
    }
    return aws_helper.add_step_to_emr_cluster(step_flow, emr_cluster_id)


def generate_bash_step(emr_cluster_id, bash_command, step_type):
    """Starts a step of type bash and returns its id.

    Keyword arguments:
    emr_cluster_id -- the id of the cluster
    bash_command -- the full bash command with relevant special characters escaped
    step_type -- the name of the step type being run (i.e. "major compaction")
    """

    console_printer.print_info(
        f"Executing bash step type '{step_type}' with command of \"{bash_command}\""
    )

    step_name = f"Automated Bash Step - {step_type}"
    return generate_local_step(emr_cluster_id, bash_command, step_name)


def generate_local_step(emr_cluster_id, command_to_run, step_name):
    """Starts a step that can run directly on local box and returns its id.

    Keyword arguments:
    emr_cluster_id -- the id of the cluster
    command_to_run -- the full command to run with relevant special characters escaped
    step_name -- the full name of the step being run
    """

    console_printer.print_info(
        f"Executing local step named '{step_name}' with command of \"{command_to_run}\""
    )

    step_flow = {
        "Name": step_name,
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["bash", "-c", command_to_run],
        },
    }
    return aws_helper.add_step_to_emr_cluster(step_flow, emr_cluster_id)
