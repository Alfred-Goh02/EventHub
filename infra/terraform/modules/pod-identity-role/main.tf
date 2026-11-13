# An IAM role exactly one Kubernetes service account, in one cluster, can assume through
# EKS Pod Identity, and the association that binds it there.
#
# NOT IRSA. IRSA trusts the cluster's OIDC issuer through an IAM OIDC provider, and this
# AWS project cannot create one: the managed SCPs of both the free and the paid plan deny
# iam:*Provider* (docs.aws.amazon.com/accounts/latest/reference/scps-and-rcps-for-projects.html).
# The first apply met it as an explicit deny on iam:CreateOpenIDConnectProvider. Pod
# Identity needs no provider: eks:* and eks-auth:* are allowed on both plans.
#
# How it runs: the eks-pod-identity-agent DaemonSet (modules/eks) answers the AWS SDK's
# container-credentials request from a pod. EKS matches the pod's service account to the
# association below and assumes this role as pods.eks.amazonaws.com, tagging the session
# with the cluster, the namespace and the service account. The service account carries no
# annotation and the pod no token file: the association is the binding.

data "aws_iam_policy_document" "trust" {
  statement {
    sid     = "AllowEksAuthToAssumeRoleForPodIdentity"
    effect  = "Allow"
    actions = ["sts:AssumeRole", "sts:TagSession"]

    principals {
      type        = "Service"
      identifiers = ["pods.eks.amazonaws.com"]
    }

    # The association names one service account already. The trust policy says the same,
    # from the session tags Pod Identity sets, so a second association made later -- by
    # anyone who may pass this role -- in another namespace or another cluster gets
    # nothing. StringEquals on all three, never StringLike: a wildcard would hand the
    # role to every service account that matched it. With session tags disabled on an
    # association there is no tag to match, and the role refuses: it fails closed.
    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/eks-cluster-arn"
      values   = [var.cluster_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/kubernetes-namespace"
      values   = [var.namespace]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/kubernetes-service-account"
      values   = [var.service_account]
    }
  }
}

resource "aws_iam_role" "this" {
  name               = var.name
  assume_role_policy = data.aws_iam_policy_document.trust.json
}

resource "aws_iam_role_policy_attachment" "this" {
  for_each = toset(var.policy_arns)

  role       = aws_iam_role.this.name
  policy_arn = each.value
}

resource "aws_iam_role_policy" "this" {
  for_each = var.inline_policies

  name   = each.key
  role   = aws_iam_role.this.id
  policy = each.value
}

# After the policies, so the first pod to ask is never handed a role with nothing on it.
resource "aws_eks_pod_identity_association" "this" {
  cluster_name    = var.cluster_name
  namespace       = var.namespace
  service_account = var.service_account
  role_arn        = aws_iam_role.this.arn

  depends_on = [
    aws_iam_role_policy_attachment.this,
    aws_iam_role_policy.this,
  ]
}
