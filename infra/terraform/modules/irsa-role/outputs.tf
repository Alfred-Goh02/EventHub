output "role_arn" {
  description = "The role's ARN: the service account's eks.amazonaws.com/role-arn annotation."
  value       = aws_iam_role.this.arn
}

output "role_name" {
  description = "The role's name."
  value       = aws_iam_role.this.name
}
