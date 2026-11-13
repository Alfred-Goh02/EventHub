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
# standard place; moving it to its own IRSA role would keep it from every other pod on
# the node, and is the next tightening when one is wanted.
resource "aws_iam_role_policy_attachment" "node" {
  for_each = toset([
    "AmazonEKSWorkerNodePolicy",
    "AmazonEKS_CNI_Policy",
    "AmazonEC2ContainerRegistryReadOnly",
  ])

  role       = aws_iam_role.node.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/${each.key}"
}

# ---------------------------------------------------------------------------
# IRSA: the cluster's OIDC issuer, trusted by IAM, so a Kubernetes service account
# can assume an IAM role (modules/irsa-role) without any node credentials.
# ---------------------------------------------------------------------------

# The thumbprint is the SHA-1 of the issuer's root CA certificate, which tls_certificate
# lists first in the chain. For an issuer whose certificate chains to a CA IAM already
# trusts, AWS documents that it validates against that CA and keeps the thumbprint only
# as configuration. Computed here rather than pasted, it is right either way.
data "tls_certificate" "oidc" {
  url = aws_eks_cluster.this.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "this" {
  url             = aws_eks_cluster.this.identity[0].oidc[0].issuer
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.oidc.certificates[0].sha1_fingerprint]
}
