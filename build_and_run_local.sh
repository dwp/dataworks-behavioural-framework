#!/bin/bash

role_arn="${1}"

export aws_sts=$(aws sts assume-role --role-arn ${role_arn} --role-session-name awscli-$(date +%m%d%y%H%M%S) --duration-seconds 900)
export secret_access_key=$(echo $aws_sts | jq .Credentials.SecretAccessKey -r)
export access_key_id=$(echo $aws_sts | jq .Credentials.AccessKeyId -r)
export session_token=$(echo $aws_sts | jq .Credentials.SessionToken -r)
