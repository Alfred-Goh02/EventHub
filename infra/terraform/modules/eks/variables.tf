variable "cluster_name" {
  description = "The EKS cluster's name, and the prefix of its IAM roles and KMS alias."
  type        = string
}

# KUBERNETES 1.37, CHOSEN ON 2026-11-13, AND WHY.
#
# This was written from knowledge of EKS releases that ran to 1.35 (early 2026), so 1.36
# and 1.37 were not taken on trust. What settled it was a primary source: amazon-eks-ami
# release v20261107 (7 November 2026) publishes AL2023 x86_64 standard node AMIs for
# 1.37.1, and for every version down to 1.31. AWS builds node AMIs for the versions EKS
# runs, so 1.37 is one EKS supports now. Of those, 1.37 because:
#
#   - it is what the charts were proven on: kind 0.33.0's node image is Kubernetes 1.37.0
#     (infra/TOOLVERSIONS.md), and kubectl 1.37.2 is within one minor version of it;
#   - the newest version has the longest standard support ahead of it (EKS gives each
#     version about 14 months). Past standard support a cluster costs $0.60 an hour
#     instead of $0.10, which is the difference between a cheap demo and an expensive one.
#
# Confirmed in the region on 2026-11-13, once there was an account to ask:
#   aws eks describe-cluster-versions --region ap-southeast-2 --profile eventhub
# listed 1.37 as STANDARD_SUPPORT until 2027-12-01, and the SSM parameter for its AL2023
# node AMI resolved (ami-04ae6dd537dbb9c44). If 1.37 ever is not listed, 1.36 is the
# fallback: the same AMI release builds it.
variable "kubernetes_version" {
  description = "The EKS Kubernetes version, major.minor. See the comment above for why the default is what it is."
  type        = string
  default     = "1.37"
  nullable    = false

  validation {
    condition     = can(regex("^1\\.[0-9]+$", var.kubernetes_version))
    error_message = "kubernetes_version is major.minor, such as 1.37."
  }
}

variable "subnet_ids" {
  description = "Private subnets, one per zone, for the cluster's network interfaces and the nodes."
  type        = list(string)

  validation {
    condition     = length(var.subnet_ids) >= 2
    error_message = "EKS needs subnets in at least two availability zones."
  }
}

# NO DEFAULT. The public endpoint answers only these ranges, and a default of 0.0.0.0/0
# would put the Kubernetes API in front of the whole internet for anyone who forgot to
# set it. Without a default, forgetting is an error at plan time instead.
variable "public_access_cidrs" {
  description = "The only ranges that may reach the cluster's public API endpoint, such as your own address as a /32."
  type        = list(string)

  validation {
    condition     = length(var.public_access_cidrs) > 0 && alltrue([for c in var.public_access_cidrs : can(cidrhost(c, 0))])
    error_message = "public_access_cidrs needs at least one valid CIDR, such as 203.0.113.10/32."
  }

  validation {
    condition     = !anytrue([for c in var.public_access_cidrs : contains(["0.0.0.0/0", "::/0"], c)])
    error_message = "public_access_cidrs may not open the API endpoint to the whole internet."
  }
}

variable "admin_principal_arn" {
  description = "The IAM user or role given cluster admin through an access entry. The role's own ARN, not an sts assumed-role ARN."
  type        = string

  validation {
    condition     = can(regex("^arn:aws:iam::[0-9]{12}:(user|role)/.+$", var.admin_principal_arn))
    error_message = "admin_principal_arn is an IAM user or role ARN: arn:aws:iam::<account>:user/<name> or :role/<name>."
  }
}

# M7I-FLEX.LARGE, BECAUSE IT IS WHAT THIS ACCOUNT CAN LAUNCH. The default was t3.medium,
# and the first apply never got a node: every launch failed "The specified instance type
# is not eligible for Free Tier" (CloudTrail, 2026-11-13). A dry run of the same launch
# had answered DryRunOperation, because a dry run checks permissions, not what the
# account's plan will launch.
#
# What the free plan launches in ap-southeast-2, asked on 2026-11-13 with
#   aws ec2 describe-instance-types --filters Name=free-tier-eligible,Values=true
# is t3, t4g and t8i in micro and small, c7i-flex.large and m7i-flex.large. t4g is Arm and
# the AMI type here is x86. The micro and small types hold 4 and 11 pods a node under the
# VPC CNI, and 1 or 2 GiB. That leaves the two large ones, 2 vCPU and 29 pods each:
# c7i-flex.large with 4 GiB at $0.1107 an hour, m7i-flex.large with 8 GiB at $0.1197
# (Price List API, Sydney). The charts request about 2 GiB for one copy of every process,
# before ArgoCD or any data store, so twice the memory for under a cent an hour.
variable "node_instance_types" {
  description = "Instance types for the managed node group. x86_64 only: the AMI type is AL2023_x86_64_STANDARD."
  type        = list(string)
  default     = ["m7i-flex.large"]
  nullable    = false
}

# The check that would have stopped the first apply before it began. The root reads the
# list from AWS; null turns the check off, for an account that launches any type.
variable "allowed_instance_types" {
  description = "When set, the instance types this account can launch: node_instance_types must be among them, checked at plan time. Null checks nothing."
  type        = list(string)
  default     = null
}

variable "node_min_size" {
  description = "The node group's minimum size."
  type        = number
  default     = 1
  nullable    = false
}

variable "node_desired_size" {
  description = "How many nodes the node group runs."
  type        = number
  default     = 2
  nullable    = false

  validation {
    condition     = var.node_min_size <= var.node_desired_size && var.node_desired_size <= var.node_max_size
    error_message = "The node group needs node_min_size <= node_desired_size <= node_max_size."
  }
}

variable "node_max_size" {
  description = "The node group's maximum size."
  type        = number
  default     = 3
  nullable    = false
}

variable "node_disk_size" {
  description = "Each node's root volume, in GiB."
  type        = number
  default     = 20
  nullable    = false
}

# Off by default, and the log group is Terraform's when it is on. EKS creates
# /aws/eks/<cluster>/cluster itself if it does not exist, with no expiry, and that group
# outlives `terraform destroy` and keeps billing for storage. Created here, it has a
# retention and goes with the cluster.
variable "control_plane_log_types" {
  description = "Control plane log types to send to CloudWatch: any of api, audit, authenticator, controllerManager, scheduler."
  type        = list(string)
  default     = []
  nullable    = false
}

variable "secrets_key_deletion_window_days" {
  description = "How long the secrets KMS key waits between `terraform destroy` and its deletion. 7 is the shortest KMS allows."
  type        = number
  default     = 7
  nullable    = false
}
