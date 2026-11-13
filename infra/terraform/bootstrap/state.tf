# THE STATE BUCKET. Every other root keeps its state here, locked with S3's own
# conditional writes (use_lockfile), so there is no DynamoDB lock table to create.
#
# State holds every resource's attributes in plain text, sometimes secrets among them,
# and losing it orphans everything it described. So the bucket is versioned (a bad write
# or a delete can be rolled back), encrypted, closed to the public by all four flags,
# reachable only over TLS, and protected from `terraform destroy`.

data "aws_caller_identity" "current" {}

locals {
  # Bucket names are global across every AWS account; the account id makes this one ours.
  state_bucket = "eventhub-tfstate-${data.aws_caller_identity.current.account_id}-${var.region}"
}

resource "aws_s3_bucket" "state" {
  bucket = local.state_bucket

  # Removing this is a deliberate edit, in a diff, before the bucket can be destroyed.
  # The teardown runbook in infra/terraform/README.md says when that is worth doing.
  lifecycle {
    prevent_destroy = true
  }
}

# ACLs off: the bucket policy is the only thing that grants or denies access.
resource "aws_s3_bucket_ownership_controls" "state" {
  bucket = aws_s3_bucket.state.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_versioning" "state" {
  bucket = aws_s3_bucket.state.id

  versioning_configuration {
    status = "Enabled"
  }
}

# SSE-S3 (AES256) rather than a KMS key of ours: the same encryption at rest, without a
# key that costs a dollar a month against a ten-dollar budget.
resource "aws_s3_bucket_server_side_encryption_configuration" "state" {
  bucket = aws_s3_bucket.state.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "state" {
  bucket = aws_s3_bucket.state.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

data "aws_iam_policy_document" "state_tls_only" {
  # Any request over plain HTTP is refused, whoever makes it.
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.state.arn, "${aws_s3_bucket.state.arn}/*"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }

  # And TLS older than 1.2.
  statement {
    sid       = "DenyOutdatedTls"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.state.arn, "${aws_s3_bucket.state.arn}/*"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "NumericLessThan"
      variable = "s3:TlsVersion"
      values   = ["1.2"]
    }
  }
}

resource "aws_s3_bucket_policy" "state" {
  bucket = aws_s3_bucket.state.id
  policy = data.aws_iam_policy_document.state_tls_only.json

  depends_on = [aws_s3_bucket_public_access_block.state]
}

# Versioning keeps every state write, and every lock file use_lockfile creates and
# deletes. Old versions go after 90 days, which is long enough to roll back a bad apply
# and short enough that the bucket does not grow for ever.
resource "aws_s3_bucket_lifecycle_configuration" "state" {
  bucket = aws_s3_bucket.state.id

  rule {
    id     = "expire-old-versions"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = 90
    }

    expiration {
      expired_object_delete_marker = true
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  depends_on = [aws_s3_bucket_versioning.state]
}
