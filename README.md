# dataworks-behavioural-framework

End to end framework for automated checks on the AWS DataWorks platform.

* [AWS DataWorks Test Strategy](docs/aws_dataworks_test_strategy.md)
* [AWS DataWorks Test Details](docs/aws_dataworks_test_details.md)
* [AWS DataWorks Manifest Comparison](docs/aws_dataworks_manifest_comparison.md)

## Setup

You will need python3 to run the tests, which is installed by default now on a UC Macbook.

When this is done, you can run the following command from the root of this repository to install everything that is needed:

`pip install -r requirements.txt --trusted-host pypi.org --trusted-host files.pythonhosted.org --user`

## Framework structure

The framework uses the `behave` python module to run "features". The features are comprised of a set of steps, which have functions attached to them.

Full details about behave can be found [on their website](https://behave.readthedocs.io/en/latest/).

## Features

The features are listed in `.feature` files and can be found in `src/features`.

Features can be tagged with `@` tags. This is used to categorise whole features or specific scenarios so that we can target specific scenarios or features when running via CI or locally.

These are the features that are currently active:

1. `administrative-processes-and-workflows` -> this contains a set of features that are used for admin environment tasks, rather than run as assurance activities
2. `end-to-end` -> this is the main full end to end set of features for the whole AWS DataWorks product
3. `historic-data-import` -> this contains features which assure the data import functionality
4. `load-test` -> features which create data loads and insert these in to the system
5. `manifest-comparison` -> see [Data Reconciliation Manifest Comparisons](docs/data-reconciliation.md)
6. `ucfs-ingestion` -> this contains features which assure the kafka ingestion functionality see [Testing Kafka Ingestion](docs/testing_kafka_ingestion.md) for more details
7. `hbase-data-loading` -> this contains scenarios which are used to kick off data loads in to HBase - either using historic or corporate data
8. `data-generation` -> used to generate datasets
9. `data-export` -> tests for exporting data to snapshots

## Steps

The steps are listed in `.py` files and can be found in `src/features/steps`. Each feature file has a corresponding steps file that contains only the steps for that feature.

Where steps are shared between multiple features they are in the `common.py` steps file.

### Step structure

A basic step looks like this:

```
@given("The text goes here which corresponds to the line in a feature file scenario {this_is_an_input}")
def step_impl(context, this_is_an_input):
    #write come code here to perform the step
```

You can use `@given`, `@when` or `@then` and the step in the feature file must start with this word. The same step can be assigned multiple `@` definitions.

Steps are generally small and consist of calling helper functions, which contain the bulk of the business logic (see [below](#helpers)).

### Context

The `context` object is a special behave object which is globally available. It is automatically passed to any step and any new variable can be assigned to it. The framework has specific rules about the context to ensure integrity and clean code practices:

1. The context object must only have variables set in the `environment.py` file or in a steps files
2. The context object must only be accessed from the steps layer - individual variables are passed to helpers

## Helpers

The bulk of the code for the framework is in the helper `.py` files in `src/helpers`. These files are classified according to functionality. The steps call many helpers to achieve their goal.

### AWS Helper

The only place to interact directly with AWS is by using the AWS helper. This helps ensure that if we moved away from AWS the changes to this framework would be minimal. We use the [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) python module for the AWS interations. We use the AWS environment variables, which must be set by the CI that runs the tests.

#### Security credentials

You MUST use these methods to interact with AWS services:

* `aws_helper.get_client(service_name=service)
* `aws_helper.get_resource(resource_name=service)

Where `service` is the name of the service, i.e "s3", "lambda" etc. If you wish to use a specific profile, you may pass that in here as `profile_name`.

These methods will get a new session object every time you get a new client or resource.

## Fixtures

Fixtures are a behave feature which allow functionality to be executed before and after a particular context. This context might be a whole feature, a single scenario or an entire test run.

To use a feature, you need to do the following:

1. Add an `@` tag to your context
2. Add a method to `src/features/fixtures/all_fixtures.py` for your fixture
3. Add a case to the switch statement in the `before_tag` method in `src/features/fixtures/environment.py` tying the tag to the method

This ensures the method you create is executed at the beginning of your tagged context. If you also (or only) want code to execute *after* the tagged context, you add it to a behave feature stack using this method:

* `context.add_cleanup([method name], [param 1], [param 2], ...)`

This puts it on a FILO stack for that specific context. Each context has its own stack.

## Environment settings

An important part of the framework is that *the tests are completely agnostic of the environment they run on*. Every setting for the environment is passed in to the tests.

When the `behave` command is executed, you can pass in user data key value pairs using `-D KEY=value`. See below for how these are passed when running locally or via CI.

These user data values are then accessed within the `src/helpers/environment_helper.py` file and transferred to corresponding variables in the `context` global object. This ensures they are available to the steps layer, which passes them through to the helpers ([above](#helpers)). 

This specific method is the only place the user data should be accessed.

### How the settings are retrieved

When you run tests either through CI or locally, the script goes to AWS Secrets Manager and retrieves an S3 location which stores json files that contain the `terraform output` for a number of our repos. These S3 files are created by the concourse pipeline `terraform-output` in the `utility` team and trigger for every repo change.

If you need to add a new repo as you need an output from that repo in your tests, then you will need to add that repo to the `terraform-output` pipeline to produce the S3 file. This code is in the `aws-common-infrastructure` repo.

Then you can add the retrieval of the json file to the `set_file_locations` method in `.run-ci.sh` in this repo. In `execute_behave` method, you can then retrieve the specific output value and pass it through to the userdata as explained above.

## CI execution

A CI job that calls the tests should call the `.run-ci.sh` script and pass in the following:

1. Meta folder location -> local folder that contains three files which detail the build that is running the tests, in concourse the `meta` resource type produces all these files and there are plenty of examples in our pipelines

## Local execution

The tests can also be executed locally against the development environment. In order to execute them, you must have valid AWS credentials with the profile name `dataworks-development` set locally and be logged in via MFA to the development environment locally.

When this is done, then in the `runners/` folder there are lots of different `.sh` scripts which can be used to execute specific tagged feature sets. The names of the scripts denote the features that will be executed.

All these scripts use a base script called `run-dev-by-tags.sh`. This script then in turns calls the `run-ci.sh` script and tells it you are running against dev. The rest of the process is exactly the same as running from CI.

### Run specific test(s) locally

In you want to run specific tests from any feature for whatever reason, then you can do the following:

1. Mark the test(s) in the feature file with a `@work-in-progress` tag
2. Run the `src/runners/run-dev-wip.sh` scripts
3. This will only run those tests

You need to ensure you do not commit the `@work-in-progress` tags to master.

## CI Pipelines

There are multiple pipelines which are released to the CI system:

1. `dataworks-behavioural-framework`
2. `manifest-comparison`
3. `load-test`

### Pipeline: dataworks-behavioural-framework

This is used to deploy the e2e fixture data to S3 after successful test runs on dev and QA. The files for this pipeline are in the ci/dataworks-behavioural-framework folder in this repo. To update this pipeline in CI, you can run the following make command:

* `make update-pipeline`

You can also pause or unpause the pipeline:

* `make pause-pipeline`
* `make unpause-pipeline`

### Pipeline: manifest-comparison

This is used to compare the import and export manifests within the desired environment - in dev and qa the manifest comparison itself it also verified (see [Data Reconciliation Manifest Comparisons](docs/data-reconciliation.md) for more details). The files for this pipeline are in the ci/manifest-comparison folder in this repo. To update this pipeline in CI, you can run the following make command:

* `make update-manifest-pipeline`

You can also pause or unpause the pipeline:

* `make pause-manifest-pipeline`
* `make unpause-manifest-pipeline`

### Pipeline: load-test

This is used run load tests in given environments - you can run the full load test or upload, import and export data separately. The files for this pipeline are in the ci/load-test folder in this repo. To update this pipeline in CI, you can run the following make command:

* `make update-load-test-pipeline`

You can also pause or unpause the pipeline:

* `make pause-load-test-pipeline`
* `make unpause-load-test-pipeline`

## Monitoring

*Any* failed run through this framework will trigger a monitoring alert to slack. It shows the failed feature and scenario and the test run. It will be either a `Test run` failed or an `Automated job` failed alert. The latter is for failed admin jobs. A test run is decided by the feature being tagged with `@test` so remember to add this for future new test teature files.

This means we get alerts when vital jobs such as scaling jobs for production services or HBase cluster maintenance fail.

To override in any pipeline, you can set the environment variable `SUPPRESS_MONITORING_ALERTS` to `true` as a string when running in concourse. However we should be careful about doing this as these alerts will be very useful for the vast majority of pipelines. It is likely we might want to override in utility pipeline that are never automatically run (i.e. majority of utility pipelines).

For local runs, this is set to `false` and can be overidden for test purposes in the `run-dev-with-tags.sh` script.

## Docker image

The GitHub Actions pipeline releases a docker container for running the framework in isolation. In order to make use of this locally, you need to build the image and then run it with AWS credentials. The easiest way to do this on a mac is to run the image and pass the environment variables with the AWS credentials in.

```
docker run -e AWS_ACCESS_KEY_ID=$${AWS_ACCESS_KEY_ID} -e AWS_SECRET_ACCESS_KEY=$${AWS_SECRET_ACCESS_KEY}
```

There is a make command to get your AWS credentials from your profile and build and run docker locally, to use this run:

```
make build-and-run
```

By default the `run-ci.sh` script will be executed using this container, but if you wish to run a different runner script you can set an environment variable and run a different make command. Any path must be from the `src/runners` directory:

```
make build-and-run-other-script SCRIPT_NAME="name_of_script"
```

You will need `aws` cli tool installed locally for this to work.

#Addendum

After cloning this repo, please run: `make bootstrap`
