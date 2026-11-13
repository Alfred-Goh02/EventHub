# An IAM role exactly one Kubernetes service account can assume, through the cluster's
# OIDC provider (IRSA). The pod gets a projected token for that service account, and STS
# exchanges it for this role's credentials: no node credentials, no static keys.

locals {
  # Trust-policy condition keys name the issuer without its scheme.
  issuer = trimprefix(var.oidc_provider_url, "https://")
}

data "aws_iam_policy_document" "trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [var.oidc_provider_arn]
    }

    # StringEquals on both keys, never StringLike. The role is for ONE service account;
    # a wildcard in :sub would hand it to every pod that can run as any matching one.
    condition {
      test     = "StringEquals"
      variable = "${local.issuer}:sub"
      values   = ["system:serviceaccount:${var.namespace}:${var.service_account}"]
    }

    # And only to tokens minted for STS, not to a token meant for some other audience.
    condition {
      test     = "StringEquals"
      variable = "${local.issuer}:aud"
      values   = ["sts.amazonaws.com"]
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
