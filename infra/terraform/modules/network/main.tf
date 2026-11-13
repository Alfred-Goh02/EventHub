# The VPC the demo cluster runs in: two availability zones, a public and a private
# subnet in each, one internet gateway and ONE NAT gateway.

# Two zones because EKS refuses a cluster whose subnets span fewer, and two is the fewest
# that survive losing one. Read from the region rather than named, so nothing here is
# tied to ap-southeast-1; opt-in-not-required leaves out Local Zones.
data "aws_availability_zones" "available" {
  state = "available"

  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

data "aws_region" "current" {}

locals {
  azs = slice(data.aws_availability_zones.available.names, 0, 2)

  # The /16, carved by cidrsubnet (with the default 10.40.0.0/16):
  #
  #   public   a /20 per zone   10.40.0.0/20, 10.40.16.0/20    the NAT and load balancers
  #   private  a /18 per zone   10.40.64.0/18, 10.40.128.0/18  nodes AND pods
  #   free                      10.40.32.0/19, 10.40.192.0/18  for the data layer, next part
  #
  # The private subnets are the big ones because the VPC CNI gives every pod an address
  # from the node's subnet: a /24 per zone would run out of pods long before it ran out
  # of nodes.
  public_cidrs  = [for i in range(2) : cidrsubnet(var.cidr, 4, i)]
  private_cidrs = [for i in range(2) : cidrsubnet(var.cidr, 2, i + 1)]
}

resource "aws_vpc" "this" {
  cidr_block = var.cidr

  # Both on, or EKS nodes cannot resolve the cluster's private endpoint.
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = var.name
  }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id

  tags = {
    Name = var.name
  }
}

# The tags are how the AWS Load Balancer Controller finds subnets: kubernetes.io/role/elb
# marks where an internet-facing load balancer may go, kubernetes.io/role/internal-elb an
# internal one, and kubernetes.io/cluster/<name> which cluster may use them. "shared"
# rather than "owned": the subnets belong to this VPC, not to the cluster.
resource "aws_subnet" "public" {
  count = 2

  vpc_id            = aws_vpc.this.id
  availability_zone = local.azs[count.index]
  cidr_block        = local.public_cidrs[count.index]

  # Nothing launched here gets a public address unasked: the nodes run in the private
  # subnets, and the NAT gateway and load balancers bring their own.
  map_public_ip_on_launch = false

  tags = {
    Name                                        = "${var.name}-public-${local.azs[count.index]}"
    "kubernetes.io/role/elb"                    = "1"
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
  }
}

resource "aws_subnet" "private" {
  count = 2

  vpc_id            = aws_vpc.this.id
  availability_zone = local.azs[count.index]
  cidr_block        = local.private_cidrs[count.index]

  tags = {
    Name                                        = "${var.name}-private-${local.azs[count.index]}"
    "kubernetes.io/role/internal-elb"           = "1"
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
  }
}

# ONE NAT GATEWAY, NOT ONE PER ZONE. This is the Field Guide's cost decision for an
# environment that exists for a few hours at a time. A NAT gateway is billed for every
# hour it exists, whether or not anything crosses it, so a second one doubles the cost
# of egress to buy one thing: that the private subnets in zone B keep reaching the
# internet while zone A is down. For a timeboxed demo that is not worth paying for. The
# price is real and accepted: if the NAT's zone fails, every node loses egress (image
# pulls from ECR, AWS APIs), and traffic from zone B crosses zones to reach it.
# Production would run one per zone, each with its own private route table.
resource "aws_eip" "nat" {
  domain = "vpc"

  tags = {
    Name = "${var.name}-nat"
  }

  depends_on = [aws_internet_gateway.this]
}

resource "aws_nat_gateway" "this" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public[0].id

  tags = {
    Name = var.name
  }

  depends_on = [aws_internet_gateway.this]
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  tags = {
    Name = "${var.name}-public"
  }
}

resource "aws_route" "public_internet" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.this.id
}

resource "aws_route_table_association" "public" {
  count = 2

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# One private route table for both zones, because there is one NAT to point it at.
resource "aws_route_table" "private" {
  vpc_id = aws_vpc.this.id

  tags = {
    Name = "${var.name}-private"
  }
}

resource "aws_route" "private_nat" {
  route_table_id         = aws_route_table.private.id
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.this.id
}

resource "aws_route_table_association" "private" {
  count = 2

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# A gateway endpoint for S3 costs nothing, and ECR serves image layers from S3: with it,
# the bulk of every image pull bypasses the NAT gateway and its per-GB charge. Only the
# manifests and the ECR API calls still go through the NAT.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${data.aws_region.current.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id]

  tags = {
    Name = "${var.name}-s3"
  }
}
