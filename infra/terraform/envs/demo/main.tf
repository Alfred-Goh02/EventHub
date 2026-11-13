# The demo environment: a VPC, an EKS cluster with one managed node group, the image
# repository, and the IAM role the AWS Load Balancer Controller runs as. No data layer
# yet: that is the next part.

locals {
  name = "eventhub-demo"
}

# WHAT THIS ACCOUNT CAN LAUNCH, ASKED OF AWS. On the free plan EC2 launches only the
# instance types it marks Free Tier-eligible, and refuses the rest at RunInstances -- which,
# for a node group, is long after the plan, inside an Auto Scaling group nobody watches.
# Read here, at plan time, the node group's precondition (modules/eks) compares against
# the account's own answer and refuses the plan instead. The list is AWS's, not a copy:
# a copy would go stale the day AWS changed its offer.
data "aws_ec2_instance_types" "launchable_on_free_plan" {
  count = var.free_plan ? 1 : 0

  filter {
    name   = "free-tier-eligible"
    values = ["true"]
  }
}

module "network" {
  source = "../../modules/network"

  name         = local.name
  cluster_name = local.name
  cidr         = var.vpc_cidr
}

module "eks" {
  source = "../../modules/eks"

  cluster_name        = local.name
  kubernetes_version  = var.kubernetes_version
  subnet_ids          = module.network.private_subnet_ids
  public_access_cidrs = var.eks_public_access_cidrs
  admin_principal_arn = var.admin_principal_arn

  node_instance_types    = var.node_instance_types
  allowed_instance_types = var.free_plan ? data.aws_ec2_instance_types.launchable_on_free_plan[0].instance_types : null
  node_min_size          = var.node_min_size
  node_desired_size      = var.node_desired_size
  node_max_size          = var.node_max_size
}

module "ecr" {
  source = "../../modules/ecr"

  name        = "eventhub/app"
  keep_images = var.ecr_keep_images

  # Every image is rebuilt from git by its SHA, so nothing in the demo's repository is
  # worth a destroy that stops halfway.
  force_delete = true
}

# THE AWS LOAD BALANCER CONTROLLER'S ROLE, through Pod Identity. Helm installs the
# controller into kube-system with the service account named here, and no role
# annotation: the association binds the role to that name. The controller then creates
# and deletes ALBs and NLBs for Ingresses and Services. It has supported Pod Identity
# since v2.7.0.
#
# The policy is a reviewed copy in the repository, not fetched at plan time, so what it
# grants changes only in a diff someone has read. It is upstream's
# docs/install/iam_policy.json from kubernetes-sigs/aws-load-balancer-controller at tag
# v3.6.3 (the latest release, 2026-11-13), read through the GitHub API and copied byte
# for byte: git blob 761d0e733c41089c91ea009dd29faf71918085e5, 8955 bytes, the same blob
# upstream has at v3.5.0, v3.6.2, v3.6.3 and main. `git hash-object` on the file prints
# that id. When the controller's chart version moves, diff this against that tag's file.
module "aws_load_balancer_controller_role" {
  source = "../../modules/pod-identity-role"

  name            = "${local.name}-aws-load-balancer-controller"
  cluster_name    = module.eks.cluster_name
  cluster_arn     = module.eks.cluster_arn
  namespace       = "kube-system"
  service_account = "aws-load-balancer-controller"

  inline_policies = {
    AWSLoadBalancerController = file("${path.module}/../../policies/aws-load-balancer-controller.json")
  }
}
