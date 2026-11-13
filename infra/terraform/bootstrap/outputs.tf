output "state_bucket" {
  description = "The state bucket. envs/demo is initialised with it: terraform init \"-backend-config=bucket=<this>\"."
  value       = aws_s3_bucket.state.bucket
}
