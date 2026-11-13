data "aws_partition" "current" {}

# ---------------------------------------------------------------------------
# The cluster's role: what the EKS control plane acts as.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "cluster_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "cluster" {
  name               = "${var.cluster_name}-cluster"
  assume_role_policy = data.aws_iam_policy_document.cluster_assume.json
}

resource "aws_iam_role_policy_attachment" "cluster" {
  role       = aws_iam_role.cluster.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonEKSClusterPolicy"
}

# AmazonEKSClusterPolicy has no KMS permissions, and the control plane uses this role to
# wrap and unwrap Secrets' data keys. Scoped to the one key.
data "aws_iam_policy_document" "cluster_secrets_key" {
  statement {
    effect    = "Allow"
    actions   = ["kms:Encrypt", "kms:Decrypt", "kms:ListGrants", "kms:DescribeKey"]
    resources = [aws_kms_key.secrets.arn]
  }
}

resource "aws_iam_role_policy" "cluster_secrets_key" {
  name   = "secrets-envelope-encryption"
  role   = aws_iam_role.cluster.id
  policy = data.aws_iam_policy_document.cluster_secrets_key.json
}

# ---------------------------------------------------------------------------
# The nodes' role: what the kubelet, the VPC CNI and image pulls act as.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "node_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "node" {
  name               = "${var.cluster_name}-node"
  assume_role_policy = data.aws_iam_policy_document.node_assume.json
}

# The three standard managed-node policies: joining the cluster, the VPC CNI's address
# management, and pulling images from ECR. The CNI's policy sits on the node role, the
# standard place; moving it to a Pod Identity role of its own (modules/pod-identity-role)
# would keep it from every other pod on the node, and is the next tightening when one is
# wanted.
#
# AmazonEKSWorkerNodePolicy also grants eks-auth:AssumeRoleForPodIdentity, which the
# eks-pod-identity-agent on each node calls for the pods it serves (read from the
# policy's default version on 2026-11-13).
resource "aws_iam_role_policy_attachment" "node" {
  for_each = toset([
    "AmazonEKSWorkerNodePolicy",
    "AmazonEKS_CNI_Policy",
    "AmazonEC2ContainerRegistryReadOnly",
  ])

  role       = aws_iam_role.node.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/${each.key}"
}

# No IAM OIDC provider, so no IRSA: this AWS project's SCPs deny iam:*Provider* on both
# plans. A service account gets an AWS role through EKS Pod Identity instead
# (modules/pod-identity-role), served by the eks-pod-identity-agent addon in main.tf.
