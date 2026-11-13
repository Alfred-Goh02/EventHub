output "cluster_name" {
  description = "The cluster's name."
  value       = aws_eks_cluster.this.name
}

output "kubernetes_version" {
  description = "The Kubernetes version the control plane runs."
  value       = aws_eks_cluster.this.version
}

output "cluster_endpoint" {
  description = "The Kubernetes API endpoint, for kubeconfig and Helm."
  value       = aws_eks_cluster.this.endpoint
}

output "cluster_certificate_authority_data" {
  description = "The cluster CA certificate, base64, as kubeconfig's certificate-authority-data takes it."
  value       = aws_eks_cluster.this.certificate_authority[0].data
}

output "cluster_security_group_id" {
  description = "The security group EKS created for the control plane and the nodes."
  value       = aws_eks_cluster.this.vpc_config[0].cluster_security_group_id
}

output "cluster_arn" {
  description = "The cluster's ARN: what a Pod Identity role's trust policy pins (modules/pod-identity-role)."
  value       = aws_eks_cluster.this.arn
}

output "node_role_arn" {
  description = "The managed node group's IAM role."
  value       = aws_iam_role.node.arn
}
