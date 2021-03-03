from helpers import aws_helper, file_helper
import os


def generate_policy_arn(aws_acc, analytical_test_e2e_role):
    """Generates an arn for the analytical test role.

    Keyword arguments:
        aws_acc -- the AWS account number
        analytical_test_e2e_role -- the role name
    """
    arn_suffix = f"{aws_acc}:role/{analytical_test_e2e_role}"
    return aws_helper.generate_arn("iam", arn_suffix)


def assume_role_for_test(aws_acc, analytical_test_e2e_role, aws_session_timeout_seconds):
    """CI assumes role for testing policy permissions

    Keyword arguments:
        aws_acc -- the AWS account number
        analytical_test_e2e_role -- the role name
        aws_session_timeout_seconds -- timeout (already in context)
    """
    arn_value = generate_policy_arn(
        aws_acc,
        analytical_test_e2e_role
    )

    aws_helper.set_details_for_role_assumption(
        arn_value, aws_session_timeout_seconds
    )
    aws_helper.clear_session()


def setup_test_file_in_s3(file_name, path, s3_bucket, timeout, tag_map):
    """Sets up a file in s3 for policy testing

    Keyword arguments:
        file_name -- the name of the file
        path -- the s3 prefix or path to where the file should be put
        s3_bucket -- the s3 bucket to upload the file to
        timeout -- the timeout to wait for consistency on s3
        tag_map -- map of tags to be associated with the s3 bucket object
    """
    local_dir = "/tmp/"

    #  Create local file, upload to s3 then delete local file
    file_helper.create_local_file(
        file_name, local_dir
    )

    aws_helper.upload_file_to_s3_and_wait_for_consistency(
        os.path.join(local_dir, file_name),
        s3_bucket,
        timeout,
        os.path.join(
            path,
            file_name,
        ),
    )

    file_helper.delete_local_file(
        file_name, local_dir
    )

    aws_helper.add_tags_to_file_in_s3(
        s3_bucket,
        os.path.join(
            path,
            file_name,
        ),
        [{"Key": tag, "Value": tag_map[tag]} for tag in tag_map],
    )
