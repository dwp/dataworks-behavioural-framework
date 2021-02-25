from helpers import aws_helper

def generate_policy_arn(aws_acc, analytical_test_e2e_role):
    """Generates an arn for the analytical test role.

    Keyword arguments:
        aws_acc -- the AWS account number
        analytical_test_e2e_role -- the role name
    """
    arn_suffix = f"{aws_acc}:role/{analytical_test_e2e_role}"
    return aws_helper.generate_arn("iam", arn_suffix)