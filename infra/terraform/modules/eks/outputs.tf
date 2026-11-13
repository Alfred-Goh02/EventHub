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

output "oidc_provider_arn" {
  description = "The IAM OIDC provider for IRSA: the Federated principal of every service-account role."
  value       = aws_iam_openid_connect_provider.this.arn
}

output "oidc_provider_url" {
  description = "The cluster's OIDC issuer URL, https:// included."
  value       = aws_eks_cluster.this.identity[0].oidc[0].issuer
}

output "node_role_arn" {
  description = "The managed node group's IAM role."
  value       = aws_iam_role.node.arn
}
