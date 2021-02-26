set +x
DURATION=${ASSUME_DURATION:=900}
AWS_STS=$(aws sts assume-role --role-arn ${AWS_ROLE_ARN} --role-session-name awscli-$(date +%m%d%y%H%M%S) --duration-seconds ${DURATION})
export AWS_SECRET_ACCESS_KEY=$(echo $AWS_STS | jq .Credentials.SecretAccessKey -r)
export AWS_ACCESS_KEY_ID=$(echo $AWS_STS | jq .Credentials.AccessKeyId -r)
export AWS_SESSION_TOKEN=$(echo $AWS_STS | jq .Credentials.SessionToken -r)
set -x
