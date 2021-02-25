resource "aws_ecr_repository" "dataworks-behavioural-framework" {
  name = "dataworks-behavioural-framework"
  tags = merge(
    local.common_tags,
    { DockerHub : "dwpdigital/dataworks-behavioural-framework" }
  )
}

resource "aws_ecr_repository_policy" "dataworks-behavioural-framework" {
  repository = aws_ecr_repository.dataworks-behavioural-framework.name
  policy     = data.terraform_remote_state.management.outputs.ecr_iam_policy_document
}

output "ecr_example_url" {
  value = aws_ecr_repository.dataworks-behavioural-framework.repository_url
}
