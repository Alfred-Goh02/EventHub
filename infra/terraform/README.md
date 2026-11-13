# Terraform: the AWS demo environment

Everything AWS needs before Kubernetes can run the charts, written resource by resource
with no registry modules. Terraform stops at the cluster boundary: nothing here installs
anything into the cluster. Helm and ArgoCD own everything inside it, the AWS Load Balancer
Controller included. Region **ap-southeast-2** (Sydney): the AWS account is on AWS's new
experience, whose project Region is set by the contact address -- Singapore maps to Sydney
-- and cannot be changed. The environment exists only for short, timeboxed demo windows;
the teardown runbook below is part of using it.

| | |
|---|---|
| `bootstrap/` | Applied once, with local state: the S3 state bucket (versioned, SSE-S3, all four public-access-block flags, TLS-only policy, `prevent_destroy`) and a $10 monthly AWS Budgets cost budget that emails at 50%, 80% and 100% actual and 100% forecast |
| `modules/network/` | A `10.40.0.0/16` VPC across two zones: a public and a private subnet in each, one internet gateway, **one** NAT gateway (the comment says why), route tables, a free S3 gateway endpoint, and the subnet tags the Load Balancer Controller discovers |
| `modules/eks/` | The cluster (Kubernetes 1.37, access entries only, private and restricted public endpoint, Secrets envelope-encrypted with KMS, standard support only), its IAM role, one managed node group (2 x t3.medium, AL2023, private subnets), the IRSA OIDC provider, the vpc-cni, kube-proxy and coredns addons, and one cluster-admin access entry |
| `modules/irsa-role/` | An IAM role exactly one service account can assume through the OIDC provider |
| `modules/ecr/` | `eventhub/app`, with immutable tags (images are tagged by git SHA), scan on push, and a keep-the-last-N lifecycle policy |
| `envs/demo/` | The modules wired together, with state in the bootstrap bucket, locked by S3 itself (`use_lockfile`) |
| `policies/` | The Load Balancer Controller's IAM policy: a reviewed copy of upstream's, at v3.6.3 (`envs/demo/main.tf` says where from) |

**Not here yet: the data layer.** RDS, ElastiCache, MSK and OpenSearch are the next part.
Until then the cluster has nothing to point the services at, so this part stands up the
platform only. `10.40.32.0/19` and `10.40.192.0/18` are left free in the VPC for them.

`node scripts/verify-terraform.mjs` checks all of it without an AWS account: formatting,
`init -backend=false` and `validate` in every root, and the guards each decision above
rests on. `--static` runs the guards alone.

## Before the first apply

```powershell
# An AWS CLI profile named eventhub. Terraform reads it from the environment;
# set this in every shell you run terraform from.
$env:AWS_PROFILE = 'eventhub'
aws sts get-caller-identity --profile eventhub

# Kubernetes 1.37 should be listed for the region (it was on 2026-11-13: standard support
# until 2027-12-01). If it is not, set kubernetes_version = "1.36" in envs/demo/terraform.tfvars.
aws eks describe-cluster-versions --region ap-southeast-2 --profile eventhub --output table

# kubectl must be 1.36 or newer for a 1.37 cluster. On this machine Docker Desktop's
# kubectl 1.34.1 comes first on PATH, ahead of winget's 1.37.2.
kubectl version --client
```

## Apply order

1. **bootstrap**, once. Its `terraform.tfstate` stays on your machine (git ignores it) and
   is the only record of the bucket and the budget: keep it.

   ```powershell
   Copy-Item infra/terraform/bootstrap/terraform.tfvars.example infra/terraform/bootstrap/terraform.tfvars
   notepad infra/terraform/bootstrap/terraform.tfvars
   terraform -chdir=infra/terraform/bootstrap init
   terraform -chdir=infra/terraform/bootstrap apply
   ```

2. **envs/demo**, for each demo window. The bucket is given at init, never written in a file.

   ```powershell
   $bucket = terraform -chdir=infra/terraform/bootstrap output -raw state_bucket
   terraform -chdir=infra/terraform/envs/demo init "-backend-config=bucket=$bucket"
   Copy-Item infra/terraform/envs/demo/terraform.tfvars.example infra/terraform/envs/demo/terraform.tfvars
   notepad infra/terraform/envs/demo/terraform.tfvars
   terraform -chdir=infra/terraform/envs/demo plan -out demo.tfplan
   terraform -chdir=infra/terraform/envs/demo apply demo.tfplan
   ```

3. **Helm, then ArgoCD**, inside the cluster. Terraform's outputs are what crosses over:

   ```powershell
   terraform -chdir=infra/terraform/envs/demo output
   aws eks update-kubeconfig --region ap-southeast-2 --name eventhub-demo --alias eventhub-demo --profile eventhub
   kubectl get nodes
   ```

   The Load Balancer Controller's chart takes `clusterName` (`cluster_name`), `region`,
   `vpcId` (`vpc_id`), and a service account `aws-load-balancer-controller` in
   `kube-system` annotated `eks.amazonaws.com/role-arn` with
   `aws_load_balancer_controller_role_arn`. Install the chart release that ships
   controller v3.6.3, the version the IAM policy is copied from. Images go to
   `ecr_repository_url`, tagged by git SHA:

   ```powershell
   $repo = terraform -chdir=infra/terraform/envs/demo output -raw ecr_repository_url
   $sha = git rev-parse --short HEAD
   aws ecr get-login-password --region ap-southeast-2 --profile eventhub | docker login --username AWS --password-stdin $repo.Split('/')[0]
   docker tag "eventhub/app:$sha" "${repo}:$sha"
   docker push "${repo}:$sha"
   ```

## What an hour costs

ap-southeast-2 on-demand list prices, read from the AWS Price List API on 2026-11-13
(`aws pricing get-products --region us-east-1`, location "Asia Pacific (Sydney)"). The
public IPv4 rate and the KMS key price are AWS's published flat rates, not read from the API.

| What | ap-southeast-2 list price | Per hour |
|---|---|---|
| EKS control plane, standard support | $0.10 per cluster-hour (extended support would be $0.60; `upgrade_policy` keeps it standard) | $0.10 |
| 2 x t3.medium, Linux, on demand | $0.0528 per instance-hour | $0.106 |
| 1 NAT gateway | $0.059 per hour, plus $0.059 per GB it processes | $0.059, plus traffic |
| The NAT's public IPv4 address | $0.005 per hour | $0.005 |
| 2 x 20 GiB node root volumes (EBS gp3) | $0.096 per GB-month | about $0.005 |
| KMS key for Secrets | $1 per key-month, prorated | about $0.001 |
| ECR | $0.10 per GB-month stored; basic scan on push | pennies a month |
| State bucket and budget | kilobytes in S3; one budget is inside the allowance AWS publishes | about $0 |
| **Total** | | **about $0.28 an hour** |

A four-hour demo window is about $1.10, plus traffic. A forgotten weekend is about $17, past
the budget. A month left running is about $200: twenty times it. Not in the table: load
balancers the controller creates once an Ingress exists (billed per hour plus usage), the
data layer (next part), and cross-zone traffic. Image layers come from S3 through the free
gateway endpoint, not through the NAT.

The budget is a backstop, not a timer. AWS refreshes cost data a few times a day, so an
alert can arrive hours after the spend. The runbook is the control.

## Teardown runbook

In this order. Terraform knows nothing about the load balancers the controller created, so
they go first, while the controller is still running to delete them.

```powershell
$env:AWS_PROFILE = 'eventhub'
aws eks update-kubeconfig --region ap-southeast-2 --name eventhub-demo --alias eventhub-demo --profile eventhub
```

1. **Delete every Ingress and every Service of type LoadBalancer**, before uninstalling
   anything with Helm. The controller deletes each ALB or NLB and then lets its object go.
   Uninstall the controller first and they are orphaned: the ALBs keep billing, and their
   security groups stop the VPC from being destroyed.

   ```powershell
   kubectl get ingress --all-namespaces
   kubectl delete ingress --all --all-namespaces
   (kubectl get svc --all-namespaces -o json | Out-String | ConvertFrom-Json).items |
     Where-Object { $_.spec.type -eq 'LoadBalancer' } |
     ForEach-Object { kubectl delete svc $_.metadata.name --namespace $_.metadata.namespace }
   kubectl get ingress,svc --all-namespaces
   ```

2. **Check that the load balancers and target groups are gone**, from AWS's side. Each
   should come back empty. Delete anything left by its ARN or id
   (`aws elbv2 delete-load-balancer`, `aws elbv2 delete-target-group`,
   `aws ec2 delete-security-group`) before going on.

   ```powershell
   aws elbv2 describe-load-balancers --region ap-southeast-2 --profile eventhub --query "LoadBalancers[].[LoadBalancerName,Type,VpcId]" --output table
   aws elbv2 describe-target-groups --region ap-southeast-2 --profile eventhub --query "TargetGroups[].[TargetGroupName,VpcId]" --output table
   aws ec2 describe-security-groups --region ap-southeast-2 --profile eventhub --filters "Name=tag-key,Values=elbv2.k8s.aws/cluster" --query "SecurityGroups[].[GroupId,GroupName,VpcId]" --output table
   ```

3. **Destroy the demo environment.** Read the plan it shows: everything in it should be
   the demo's, and nothing from bootstrap.

   ```powershell
   terraform -chdir=infra/terraform/envs/demo destroy
   ```

4. **Look for what outlived it**: unattached EBS volumes, network interfaces, and
   addresses. A volume or interface tagged `kubernetes.io/cluster/eventhub-demo` is the
   cluster's leftover; delete it with `aws ec2 delete-volume --volume-id <id>` or
   `aws ec2 delete-network-interface --network-interface-id <id>` (same `--region` and
   `--profile`). An address with no association is billing for nothing.

   ```powershell
   aws ec2 describe-volumes --region ap-southeast-2 --profile eventhub --filters "Name=status,Values=available" --query "Volumes[].[VolumeId,Size,CreateTime]" --output table
   aws ec2 describe-network-interfaces --region ap-southeast-2 --profile eventhub --filters "Name=status,Values=available" --query "NetworkInterfaces[].[NetworkInterfaceId,VpcId,Description]" --output table
   aws ec2 describe-addresses --region ap-southeast-2 --profile eventhub --query "Addresses[].[PublicIp,AllocationId,AssociationId]" --output table
   aws logs describe-log-groups --region ap-southeast-2 --profile eventhub --log-group-name-prefix /aws/eks/eventhub-demo --query "logGroups[].logGroupName" --output table
   ```

   The KMS key is not deleted at once: `destroy` schedules it, seven days out.

5. **Keep the bootstrap bucket and budget.** They cost close to nothing, the budget keeps
   watching the account, and the next demo window needs the bucket. Only when finished
   with AWS for good, and last of all: empty every version from the bucket, remove the
   `lifecycle { prevent_destroy = true }` block from `bootstrap/state.tf` (a deliberate
   edit, which `verify-terraform.mjs` fails on for as long as it lasts), destroy, and put
   the block back.

   ```powershell
   $bucket = terraform -chdir=infra/terraform/bootstrap output -raw state_bucket
   $listing = aws s3api list-object-versions --bucket $bucket --profile eventhub --output json | Out-String | ConvertFrom-Json
   foreach ($v in @($listing.Versions) + @($listing.DeleteMarkers)) {
     if ($v) { aws s3api delete-object --bucket $bucket --key $v.Key --version-id $v.VersionId --profile eventhub | Out-Null }
   }
   terraform -chdir=infra/terraform/bootstrap destroy
   git checkout -- infra/terraform/bootstrap/state.tf
   ```
