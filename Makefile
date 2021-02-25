SHELL:=/bin/bash

default: help

aws_profile=default
aws_region=eu-west-2

.PHONY: help
help:
	@{ \
		grep -E '^[a-zA-Z_-]+:.*?## .*$$' Makefile | \
		sort | \
		awk \
		'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'; \
	}

.PHONY: bootstrap
bootstrap: ## Bootstrap local environment for first use
	@make git-hooks
	pip3 install --user Jinja2 PyYAML boto3
	@{ \
		export AWS_PROFILE=$(aws_profile); \
		export AWS_REGION=$(aws_region); \
		python3 bootstrap_terraform.py; \
	}

.PHONY: git-hooks
git-hooks: ## Set up hooks in .githooks
	@git submodule update --init .githooks ; \
	git config core.hooksPath .githooks \

.PHONY: concourse-login
concourse-login: ## Login to concourse using Fly
	fly -t aws-concourse login -c https://ci.dataworks.dwp.gov.uk/ -n dataworks

.PHONY: utility-login
utility-login: ## Login to utility team using Fly
	fly -t utility login -c https://ci.dataworks.dwp.gov.uk/ -n utility

.PHONY: update-pipeline
update-pipeline: ## Update the main pipeline
	aviator

.PHONY: update-manifest-pipeline
update-manifest-pipeline: ## Update the manifest comparison pipeline
	aviator -f aviator-manifest-comparison.yml

.PHONY: update-load-test-pipeline
update-load-test-pipeline: ## Update the load-test pipeline
	aviator -f aviator-load-tests.yml

.PHONY: pause-pipeline
pause-pipeline: ## Pause the main pipeline
	fly --target aws-concourse pause-pipeline --pipeline dataworks-behavioural-framework

.PHONY: pause-manifest-pipeline
pause-manifest-pipeline: ## Pause the manifest comparison pipeline
	fly --target utility pause-pipeline --pipeline manifest-comparison

.PHONY: pause-load-test-pipeline
pause-load-test-pipeline: ## Pause the load-test pipeline
	fly --target utility pause-pipeline --pipeline load-test

.PHONY: unpause-pipeline
unpause-pipeline: ## Unpause the main pipeline
	fly --target aws-concourse unpause-pipeline --pipeline dataworks-behavioural-framework

.PHONY: unpause-manifest-pipeline
unpause-manifest-pipeline: ## Unpause the manifest comparison pipeline
	fly --target utility unpause-pipeline --pipeline manifest-comparison

.PHONY: unpause-load-test-pipeline
unpause-load-test-pipeline: ## Unpause the load-test pipeline
	fly --target utility unpause-pipeline --pipeline load-test

.PHONY: build
build:
	docker build -t e2e-framework .

.PHONY: build-and-run
build-and-run:
	$(eval role_arn=$(shell aws --profile dataworks-development configure get role_arn))
	source ./build_and_run_local.sh $(role_arn) && \
	docker build -t e2e-framework . && docker run --network=host -e OVERRIDE_ROLE=true \
	-e AWS_DEFAULT_REGION="eu-west-2" -e AWS_ACCESS_KEY_ID="$${access_key_id}" \
	-e AWS_SECRET_ACCESS_KEY="$${secret_access_key}" -e AWS_SESSION_TOKEN="$${session_token}" \
	-e AWS_ROLE_ARN="$${role_arn}" e2e-framework

.PHONY: build-and-run-other-script
build-and-run-other-script:
	$(eval role_arn=$(shell aws --profile dataworks-development configure get role_arn))
	source ./build_and_run_local.sh $(role_arn) && \
	docker build -t e2e-framework . && docker run --network=host -e OVERRIDE_ROLE=true \
	-e AWS_DEFAULT_REGION="eu-west-2" -e AWS_ACCESS_KEY_ID="$${access_key_id}" \
	-e AWS_SECRET_ACCESS_KEY="$${secret_access_key}" -e AWS_SESSION_TOKEN="$${session_token}" \
	-e AWS_ROLE_ARN="$${role_arn}" e2e-framework $(SCRIPT_NAME)

.PHONY: terraform-workspace-new
terraform-workspace-new: ## Creates new Terraform workspace with Concourse remote execution
	declare -a workspace=( management ) \
	make bootstrap ; \
	cp terraform.tf workspaces.tf && \
	for i in "$${workspace[@]}" ; do \
		fly -t aws-concourse execute --config create-workspace.yml --input repo=. -v workspace="$$i" ; \
	done
	rm workspaces.tf
