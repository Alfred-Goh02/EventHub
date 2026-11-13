variable "name" {
  description = "The IAM role's name."
  type        = string
}

variable "oidc_provider_arn" {
  description = "The cluster's IAM OIDC provider (modules/eks output oidc_provider_arn)."
  type        = string
}

variable "oidc_provider_url" {
  description = "The cluster's OIDC issuer URL (modules/eks output oidc_provider_url). https:// is optional."
  type        = string
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
