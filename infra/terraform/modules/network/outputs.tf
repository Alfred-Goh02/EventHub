output "vpc_id" {
  description = "The VPC. The AWS Load Balancer Controller's Helm values take it as vpcId."
  value       = aws_vpc.this.id
}

output "vpc_cidr" {
  description = "The VPC's IPv4 range."
  value       = aws_vpc.this.cidr_block
}

output "azs" {
  description = "The two availability zones, in subnet order."
  value       = local.azs
}

output "public_subnet_ids" {
  description = "One public subnet per zone: the NAT gateway and internet-facing load balancers."
  value       = aws_subnet.public[*].id
}

output "private_subnet_ids" {
  description = "One private subnet per zone: the EKS nodes, their pods, and the cluster's own network interfaces."
  value       = aws_subnet.private[*].id
}

output "nat_public_ip" {
  description = "The one address every pod's outbound traffic leaves from."
  value       = aws_eip.nat.public_ip
}
