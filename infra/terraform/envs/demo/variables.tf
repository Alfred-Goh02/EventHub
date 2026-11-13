variable "region" {
  description = "The region the demo runs in."
  type        = string
  default     = "ap-southeast-1"
}

variable "aws_account_id" {
  description = "The only AWS account this may be applied to. Unset, any account the credentials reach is accepted."
  type        = string
  default     = null

  validation {
    condition     = var.aws_account_id == null || can(regex("^[0-9]{12}$", var.aws_account_id))
    error_message = "aws_account_id is twelve digits."
  }
}

# No default, so forgetting it is an error rather than an open endpoint. modules/eks
# refuses 0.0.0.0/0 as well.
variable "eks_public_access_cidrs" {
  description = "The only ranges that may reach the cluster's public API endpoint: your own address as a /32."
  type        = list(string)
}

variable "admin_principal_arn" {
  description = "The IAM user or role given cluster admin. The role's own ARN, not an sts assumed-role ARN."
  type        = string
}

# Overrides. Each defaults to null, which hands the module its own default, so every
# default is written in exactly one place: the module.

variable "vpc_cidr" {
  description = "The VPC's /16. Null takes modules/network's default, 10.40.0.0/16."
  type        = string
  default     = null
}

variable "kubernetes_version" {
  description = "The EKS Kubernetes version. Null takes modules/eks's default, which says why it is that version."
  type        = string
  default     = null
}

variable "node_instance_types" {
  description = "Node instance types. Null takes modules/eks's default, t3.medium."
  type        = list(string)
  default     = null
}

variable "node_min_size" {
  description = "Null takes modules/eks's default."
  type        = number
  default     = null
}

variable "node_desired_size" {
  description = "How many nodes. Null takes modules/eks's default, 2."
  type        = number
  default     = null
}

variable "node_max_size" {
  description = "Null takes modules/eks's default."
  type        = number
  default     = null
}

variable "ecr_keep_images" {
  description = "How many images ECR keeps. Null takes modules/ecr's default."
  type        = number
  default     = null
}
