# Credentials come from the environment, never from this file: set
# $env:AWS_PROFILE = 'eventhub' before running terraform (see infra/terraform/README.md).
provider "aws" {
  region = var.region

  # Refuses to plan or apply against any other account, once aws_account_id is set.
  allowed_account_ids = var.aws_account_id == null ? null : [var.aws_account_id]

  # On every resource this provider creates, so the bill can be read by project and
  # environment. Not on what the cluster creates for itself: load balancers the
  # controller makes carry its own elbv2.k8s.aws/cluster tag instead, which the teardown
  # runbook looks for.
  default_tags {
    tags = {
      Project     = "eventhub"
      Environment = "demo"
      ManagedBy   = "terraform"
    }
  }
}
