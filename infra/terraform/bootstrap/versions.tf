terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.73"
    }
  }

  # NO BACKEND: local state, on purpose. This root creates the bucket every other root
  # keeps its state in, so it cannot keep its own state there before it exists. It is
  # applied once, and its terraform.tfstate (ignored by git) is the record of the bucket
  # and the budget: keep it. Lost, the two can be brought back under management with
  # `terraform import`; the bucket's prevent_destroy means nothing is lost with it.
}
