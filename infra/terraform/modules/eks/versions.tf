terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.0"
    }
    # For the OIDC issuer's certificate thumbprint (data.tls_certificate in iam.tf).
    tls = {
      source  = "hashicorp/tls"
      version = ">= 4.0"
    }
  }
}
