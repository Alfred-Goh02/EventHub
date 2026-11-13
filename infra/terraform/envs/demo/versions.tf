terraform {
  # use_lockfile (S3-native state locking) arrived in Terraform 1.10.
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.73"
    }
  }

  # STATE IN THE BOOTSTRAP BUCKET, LOCKED BY S3 ITSELF. use_lockfile takes the lock with
  # a conditional write of <key>.tflock beside the state, so two applies cannot
  # interleave, with no DynamoDB table to create or pay for.
  #
  # The bucket is not written here: its name carries the account id, and this file is
  # the same for any account. It is given at init, from the bootstrap root's output:
  #   terraform -chdir=infra/terraform/envs/demo init "-backend-config=bucket=<state_bucket>"
  backend "s3" {
    key          = "envs/demo/terraform.tfstate"
    region       = "ap-southeast-2"
    encrypt      = true
    use_lockfile = true
  }
}
