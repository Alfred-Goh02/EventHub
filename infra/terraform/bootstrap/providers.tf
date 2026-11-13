# Credentials come from the environment, never from this file: set
# $env:AWS_PROFILE = 'eventhub' before running terraform (see infra/terraform/README.md).
provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = "eventhub"
      Environment = "bootstrap"
      ManagedBy   = "terraform"
    }
  }
}
