variable "name" {
  description = "The IAM role's name."
  type        = string
}

variable "cluster_name" {
  description = "The EKS cluster the association is made in (modules/eks output cluster_name)."
  type        = string
}

variable "cluster_arn" {
  description = "The same cluster's ARN (modules/eks output cluster_arn): the trust policy admits sessions tagged with it and no other."
  type        = string

  validation {
    condition     = can(regex("^arn:aws:eks:[a-z0-9-]+:[0-9]{12}:cluster/.+$", var.cluster_arn))
    error_message = "cluster_arn is an EKS cluster ARN: arn:aws:eks:<region>:<account>:cluster/<name>."
  }
}

variable "namespace" {
  description = "The namespace of the one service account that may assume the role."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$", var.namespace))
    error_message = "namespace is a Kubernetes namespace name, with no wildcard."
  }
}

variable "service_account" {
  description = "The name of the one service account that may assume the role."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9]([-.a-z0-9]*[a-z0-9])?$", var.service_account))
    error_message = "service_account is a Kubernetes service account name, with no wildcard."
  }
}

variable "policy_arns" {
  description = "Managed policies to attach to the role."
  type        = list(string)
  default     = []
  nullable    = false
}

variable "inline_policies" {
  description = "Inline policies to put on the role, as name => policy JSON."
  type        = map(string)
  default     = {}
  nullable    = false

  validation {
    condition     = alltrue([for json in values(var.inline_policies) : can(jsondecode(json))])
    error_message = "Every inline policy must be valid JSON."
  }
}
