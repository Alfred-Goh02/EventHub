output "role_arn" {
  description = "The role's ARN. Nothing in the cluster needs it: the association binds the role to the service account."
  value       = aws_iam_role.this.arn
}

output "role_name" {
  description = "The role's name."
  value       = aws_iam_role.this.name
}

output "association_id" {
  description = "The Pod Identity association's id."
  value       = aws_eks_pod_identity_association.this.association_id
}
