import json
import os.path

from behave import then

from helpers import aws_helper, console_printer, object_tagger_helper


@then("the '{data_product}' object_tagger has run successfully")
def step_impl(context, data_product):
    if data_product in ["pdm", "uc_feature"]:
        job_queue_name = data_product + "_object_tagger"
    else:
        raise AssertionError(
            f"Data product {data_product} not defined in step 'the {{data_product}}"
            f" object_tagger has run successfully'"
        )

    job_ids = aws_helper.poll_batch_queue_for_job(
        job_queue_name=job_queue_name,
        timeout_in_seconds=300,
        job_definition_names=["s3_object_tagger_job"],
    )
    if len(job_ids) > 1:
        console_printer.print_warning_text(
            f"Multiple job ids found for {job_queue_name}.  Waiting for all to finish"
        )
    for job_id in job_ids:
        status = aws_helper.poll_batch_job_status(job_id=job_id, timeout_in_seconds=300)
        console_printer.print_info("\n")
        assert status == "SUCCEEDED"


@then("the correct tags are applied to the '{data_product}' data")
def step_impl(context, data_product):
    common_config_bucket = context.common_config_bucket
    published_bucket = context.published_bucket

    if data_product == "uc_feature":
        configuration = json.loads(context.uc_feature_data_classification)
    elif data_product == "pdm":
        configuration = json.loads(context.pdm_data_classification)
    else:
        raise AssertionError(
            f"Data product not defined under step 'the correct tags are applied to the"
            f" {{data_product}} data'"
        )

    output_prefix = configuration["data_s3_prefix"]
    rbac_file_path = os.path.join(
        configuration["config_prefix"], configuration["config_file"]
    )
    if not output_prefix[-1] == "/":
        output_prefix += "/"

    all_rbac_tags = object_tagger_helper.get_rbac_csv_tags(
        rbac_csv_s3_bucket=common_config_bucket, rbac_csv_s3_key=rbac_file_path
    )

    s3_keys = aws_helper.get_s3_file_object_keys_matching_pattern(
        s3_bucket=published_bucket,
        s3_prefix=output_prefix,
    )

    keys_with_tags = [
        (
            key,
            object_tagger_helper.rbac_required_tags(key, tags_dict=all_rbac_tags),
            aws_helper.get_tags_of_file_in_s3(s3_bucket=published_bucket, s3_key=key)[
                "TagSet"
            ],
        )
        for key in s3_keys
    ]

    for key, required_tags, actual_tags in keys_with_tags:
        assert object_tagger_helper.aws_tags_are_subset(
            required_tags, actual_tags
        ), "Required tags not applied to s3 objects"
