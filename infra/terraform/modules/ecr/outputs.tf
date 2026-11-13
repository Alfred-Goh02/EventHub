output "repository_url" {
  description = "Where images are pushed and pulled: <account>.dkr.ecr.<region>.amazonaws.com/<name>."
  value       = aws_ecr_repository.this.repository_url
}

output "repository_arn" {
  description = "The repository's ARN."
  value       = aws_ecr_repository.this.arn
}
