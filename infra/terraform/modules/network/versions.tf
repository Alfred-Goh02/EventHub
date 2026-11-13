terraform {
  required_version = ">= 1.10"

  # A module states the least it needs; the root that calls it pins the range and
  # commits the lock file.
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.0"
    }
  }
}
