# The demo environment: a VPC, an EKS cluster with one managed node group, the image
# repository, the IAM role the AWS Load Balancer Controller runs as, and the EBS CSI
# driver the data stores' disks come from. The stores themselves run in the cluster, from
# infra/stores (a Helm chart): Terraform stops at the cluster boundary.

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

# THE EBS CSI DRIVER: every disk a data store has. Postgres, OpenSearch, ClickHouse and
# Kafka each claim a gp3 volume through infra/stores' StorageClass, and without the
# driver every claim stays Pending.
#
# ITS ROLE, BY POD IDENTITY, BOUND BEFORE ITS PODS EXIST. The controller calls
# CreateVolume, AttachVolume and the rest; nothing else in the add-on calls AWS. Pod
# Identity hands a pod its credentials when the pod is admitted, so a controller started
# before its association would run without any until restarted -- and the node role, the
# only other way out, has no EBS permissions and is closed to pods by IMDS hop limit 1.
# Hence depends_on below. And not the add-on's own pod_identity_association block: the
# module makes the association, with the trust policy pinned to this one service
# account, and a cluster holds one association per service account.
#
# AmazonEBSCSIDriverPolicyV2, the policy EKS recommends for this add-on version
# (describe-addon-configuration, podIdentityConfiguration, 2026-11-14). Unlike the older
# AmazonEBSCSIDriverPolicy it lets the driver modify, attach and delete only volumes
# carrying the tag it puts on the ones it creates. It has no KMS statements, which is why
# infra/stores encrypts with the AWS-managed aws/ebs key and not a key of ours.
module "ebs_csi_driver_role" {
  source = "../../modules/pod-identity-role"

  name            = "${local.name}-ebs-csi-driver"
  cluster_name    = module.eks.cluster_name
  cluster_arn     = module.eks.cluster_arn
  namespace       = "kube-system"
  service_account = "ebs-csi-controller-sa"

  policy_arns = ["arn:aws:iam::aws:policy/AmazonEBSCSIDriverPolicyV2"]
}

# AFTER THE NODES, like coredns: the controller is a Deployment, and with no node to run
# on the add-on stays degraded until Terraform times out. module.eks as a whole includes
# the node group.
#
# The configuration, each key checked against the add-on's own schema
# (describe-addon-configuration for v1.68.0-eksbuild.1):
#   - one controller replica, not two: pods are this cluster's tight limit (29 a node);
#   - extraVolumeTags, so every volume the driver makes carries the project's tags and the
#     teardown's leftover check finds it by them;
#   - --k8s-tag-cluster-id, so each volume also says which cluster made it;
#   - AWS_REGION, so the controller never has to ask IMDS, which pods cannot reach here.
resource "aws_eks_addon" "ebs_csi_driver" {
  cluster_name                = module.eks.cluster_name
  addon_name                  = "aws-ebs-csi-driver"
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"

  configuration_values = jsonencode({
    controller = {
      replicaCount    = 1
      extraVolumeTags = { Project = "eventhub", Environment = "demo" }
      additionalArgs  = ["--k8s-tag-cluster-id=${local.name}"]
      env             = [{ name = "AWS_REGION", value = var.region }]
    }
  })

  depends_on = [
    module.eks,
    module.ebs_csi_driver_role,
  ]
}
