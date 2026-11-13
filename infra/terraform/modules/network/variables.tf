variable "name" {
  description = "Prefix for every resource's Name tag."
  type        = string
}

variable "cluster_name" {
  description = "The EKS cluster these subnets serve. The AWS Load Balancer Controller only considers subnets tagged kubernetes.io/cluster/<this name>."
  type        = string
}

variable "cidr" {
  description = "The VPC's IPv4 range. The subnet layout in main.tf is carved from it with cidrsubnet and assumes a /16."
  type        = string
  default     = "10.40.0.0/16"
  nullable    = false

  validation {
    condition     = can(cidrhost(var.cidr, 0)) && endswith(var.cidr, "/16")
    error_message = "cidr must be an IPv4 /16, such as 10.40.0.0/16: the subnet layout in main.tf assumes one."
  }
}
