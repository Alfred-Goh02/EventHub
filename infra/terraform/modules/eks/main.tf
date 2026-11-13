# One EKS cluster and one managed node group, and nothing inside the cluster: Terraform
# stops at the cluster boundary. Everything that runs in Kubernetes, the AWS Load
# Balancer Controller included, is Helm's and ArgoCD's.

# ENVELOPE ENCRYPTION OF SECRETS. EKS encrypts etcd at rest regardless; this adds a key
# of ours around each Secret's data key, so reading a Secret out of etcd also takes
# kms:Decrypt on this key. A customer-managed key costs about $1 a month, prorated.
resource "aws_kms_key" "secrets" {
  description             = "Envelope encryption of Kubernetes Secrets in ${var.cluster_name}"
  enable_key_rotation     = true
  deletion_window_in_days = var.secrets_key_deletion_window_days
}

resource "aws_kms_alias" "secrets" {
  name          = "alias/${var.cluster_name}-secrets"
  target_key_id = aws_kms_key.secrets.key_id
}

resource "aws_cloudwatch_log_group" "cluster" {
  count = length(var.control_plane_log_types) > 0 ? 1 : 0

  name              = "/aws/eks/${var.cluster_name}/cluster"
  retention_in_days = 7
}

resource "aws_eks_cluster" "this" {
  name     = var.cluster_name
  version  = var.kubernetes_version
  role_arn = aws_iam_role.cluster.arn

  # Access entries only (authentication mode API): no aws-auth ConfigMap. And the
  # identity that runs `terraform apply` gets no implicit admin. Cluster admin is the one
  # access entry below, so who can run kubectl is written in this file rather than being
  # whoever happened to apply it. Both settings force a new cluster if changed later.
  access_config {
    authentication_mode                         = "API"
    bootstrap_cluster_creator_admin_permissions = false
  }

  # vpc-cni, kube-proxy and coredns are installed as managed addons below. Left true, EKS
  # would also install its own unmanaged copies first, for the addons to overwrite.
  bootstrap_self_managed_addons = false

  # Private access so the nodes reach the API inside the VPC; public access so kubectl
  # and Helm reach it from outside, and only from public_access_cidrs.
  vpc_config {
    subnet_ids              = var.subnet_ids
    endpoint_private_access = true
    endpoint_public_access  = true
    public_access_cidrs     = var.public_access_cidrs
  }

  encryption_config {
    resources = ["secrets"]

    provider {
      key_arn = aws_kms_key.secrets.arn
    }
  }

  # STANDARD, NOT THE DEFAULT EXTENDED. When a version leaves standard support, an
  # EXTENDED cluster stays on it at $0.60 an hour, six times the price; a STANDARD one
  # is upgraded by EKS instead. A demo is better off upgraded than billed.
  upgrade_policy {
    support_type = "STANDARD"
  }

  enabled_cluster_log_types = var.control_plane_log_types

  depends_on = [
    aws_iam_role_policy_attachment.cluster,
    aws_iam_role_policy.cluster_secrets_key,
    aws_cloudwatch_log_group.cluster,
  ]
}

# THE CORE ADDONS. vpc-cni and kube-proxy before the nodes, because a node without a CNI
# never becomes Ready. coredns after them, because it is a Deployment: with no node to
# schedule on it stays degraded, and Terraform would wait on it until it timed out.
#
# No addon_version: EKS installs the default version for the cluster's Kubernetes
# version. Pinning one is a deliberate upgrade, done here, when one is wanted.
resource "aws_eks_addon" "before_nodes" {
  for_each = toset(["vpc-cni", "kube-proxy"])

  cluster_name                = aws_eks_cluster.this.name
  addon_name                  = each.key
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"
}

resource "aws_eks_node_group" "default" {
  cluster_name    = aws_eks_cluster.this.name
  node_group_name = "default"
  version         = aws_eks_cluster.this.version
  node_role_arn   = aws_iam_role.node.arn

  # Private subnets only: a node has no public address and reaches out through the NAT.
  subnet_ids = var.subnet_ids

  # Amazon Linux 2023 on x86. EKS publishes no Amazon Linux 2 AMIs for 1.33 or later.
  ami_type       = "AL2023_x86_64_STANDARD"
  capacity_type  = "ON_DEMAND"
  instance_types = var.node_instance_types
  disk_size      = var.node_disk_size

  scaling_config {
    min_size     = var.node_min_size
    desired_size = var.node_desired_size
    max_size     = var.node_max_size
  }

  update_config {
    max_unavailable = 1
  }

  # With authentication mode API, EKS creates the node role's access entry itself
  # (type EC2_LINUX) when the node group is created; nothing to add for it here.
  depends_on = [
    aws_iam_role_policy_attachment.node,
    aws_eks_addon.before_nodes,
  ]
}

resource "aws_eks_addon" "coredns" {
  cluster_name                = aws_eks_cluster.this.name
  addon_name                  = "coredns"
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"

  depends_on = [aws_eks_node_group.default]
}

# CLUSTER ADMIN, FOR ONE PRINCIPAL. The only way into the cluster that this file grants.
resource "aws_eks_access_entry" "admin" {
  cluster_name  = aws_eks_cluster.this.name
  principal_arn = var.admin_principal_arn
  type          = "STANDARD"
}

resource "aws_eks_access_policy_association" "admin" {
  cluster_name  = aws_eks_cluster.this.name
  principal_arn = aws_eks_access_entry.admin.principal_arn
  policy_arn    = "arn:${data.aws_partition.current.partition}:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"

  access_scope {
    type = "cluster"
  }
}
