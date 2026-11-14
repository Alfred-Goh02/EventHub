# What Helm and kubectl need from this environment. Terraform stops at the cluster
# boundary; these are the values that cross it.

output "region" {
  description = "The region everything is in."
  value       = var.region
}

output "cluster_name" {
  description = "The EKS cluster: the AWS Load Balancer Controller's clusterName value."
  value       = module.eks.cluster_name
}

output "cluster_endpoint" {
  description = "The Kubernetes API endpoint."
  value       = module.eks.cluster_endpoint
}

output "cluster_certificate_authority_data" {
  description = "The cluster CA certificate, base64."
  value       = module.eks.cluster_certificate_authority_data
}

output "cluster_arn" {
  description = "The cluster's ARN, for any further Pod Identity role (modules/pod-identity-role)."
  value       = module.eks.cluster_arn
}

output "vpc_id" {
  description = "The VPC: the AWS Load Balancer Controller's vpcId value."
  value       = module.network.vpc_id
}

output "nat_public_ip" {
  description = "The address all of the cluster's outbound traffic leaves from."
  value       = module.network.nat_public_ip
}

output "ecr_repository_url" {
  description = "Where eventhub/app images are pushed, tagged by git SHA."
  value       = module.ecr.repository_url
}

output "ecr_web_repository_url" {
  description = "Where eventhub/web images (the web app) are pushed, tagged by git SHA."
  value       = module.ecr_web.repository_url
}

output "aws_load_balancer_controller_role_arn" {
  description = "The role the controller runs as. Pod Identity binds it to kube-system/aws-load-balancer-controller; the service account needs no annotation."
  value       = module.aws_load_balancer_controller_role.role_arn
}

output "kubeconfig_command" {
  description = "Adds this cluster to kubeconfig."
  value       = "aws eks update-kubeconfig --region ${var.region} --name ${module.eks.cluster_name} --alias ${module.eks.cluster_name} --profile eventhub"
}
