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
| `modules/eks/` | The cluster (Kubernetes 1.37, access entries only, private and restricted public endpoint, Secrets envelope-encrypted with KMS, standard support only), its IAM role, one managed node group (2 x m7i-flex.large, AL2023, private subnets, its types checked at plan time against what the account can launch), the vpc-cni, kube-proxy, eks-pod-identity-agent and coredns addons, and one cluster-admin access entry |
| `modules/pod-identity-role/` | An IAM role exactly one service account in one cluster can assume through EKS Pod Identity, and the association that binds it. Not IRSA: the AWS project's SCPs deny `iam:*Provider*`, so an OIDC provider cannot be created |
| `modules/ecr/` | `eventhub/app`, with immutable tags (images are tagged by git SHA), scan on push, and a keep-the-last-N lifecycle policy |
| `envs/demo/` | The modules wired together, with state in the bootstrap bucket, locked by S3 itself (`use_lockfile`), plus the EBS CSI driver add-on and its Pod Identity role (`AmazonEBSCSIDriverPolicyV2`), which every data store's disk comes from |
| `policies/` | The Load Balancer Controller's IAM policy: a reviewed copy of upstream's, at v3.6.3 (`envs/demo/main.tf` says where from) |

**The data layer runs inside the cluster**, from `infra/stores`, a Helm chart: Postgres,
Redis, OpenSearch, ClickHouse, Kafka, Schema Registry and Mailpit, each as compose runs it,
the four that keep data on encrypted gp3 disks from the EBS CSI driver this root installs.
Not RDS, ElastiCache or OpenSearch Service, by decision (2026-11-14): the free plan's SCP
leaves out `kafka`, so MSK was never an option, and the stores fit on the two nodes.
`10.40.32.0/19` and `10.40.192.0/18` stay free in the VPC should a managed store come later.
`infra/eks/up.mjs` installs the rest on top of them: the image, the services' Secrets,
setup as a Job (`infra/setup`), the fifteen services, and the gateway (`infra/kong`).

**What the account allows.** Two limits shaped this directory, from two places:

- **The project's managed SCPs**, published in full at
  [Managed policies for your organization](https://docs.aws.amazon.com/accounts/latest/reference/scps-and-rcps-for-projects.html).
  They deny `iam:*Provider*` on both plans, so no IAM OIDC provider and no IRSA.
- **The free plan itself.** EC2 launches only the instance types
  `aws ec2 describe-instance-types --filters Name=free-tier-eligible,Values=true` lists, and
  refuses the rest at `RunInstances`. No SCP says so. `free_plan` in `envs/demo` defaults to
  true and checks the node group against that list at plan time; set it false after
  upgrading.

`node scripts/verify-terraform.mjs` checks all of it without an AWS account: formatting,
`init -backend=false` and `validate` in every root, each root's `terraform test` (plans
against a mocked provider: `envs/demo/tests` watches the node-type check refuse), and the
guards each decision above rests on. `--static` runs the guards alone.

## Before the first apply

**A login that renews itself.** `aws login` hands out credentials that last 15 minutes and
are renewed by the CLI. Terraform read them once and did not renew them, so the first apply
died halfway with `ExpiredTokenException`; an EKS cluster alone takes ten minutes or more.
A profile whose `credential_process` asks the CLI each time is renewed on every request.
Add it to `~/.aws/config` once, beside the `eventhub` profile `aws login` made:

```ini
[profile eventhub-tf]
region = ap-southeast-2
credential_process = aws configure export-credentials --profile eventhub --format process
```

```powershell
# Terraform reads the profile from the environment; set this in every shell you run
# terraform from. The aws commands below keep --profile eventhub.
$env:AWS_PROFILE = 'eventhub-tf'
aws sts get-caller-identity --profile eventhub-tf

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

   Then EventHub itself, with one command, from a clean working tree (the images are tagged
   with the commit they are built from). It builds and pushes the backend's image to
   `ecr_repository_url` and the web app's to `ecr_web_repository_url`, by git SHA; makes the
   cluster's own credentials -- every password, secret and both keypairs, kept in the
   Secret `eventhub-credentials` and reused by every later run -- and every Secret the
   stores and services read, from compose with those in place of compose's development
   ones; installs the stores (`infra/stores`) and waits for them; runs setup, compose's own
   setup command as a Job;
   installs the fifteen charts one replica each, then Kong in front of them, then the web
   app; asks each service's `/ready` from inside the cluster; sends requests through Kong
   that only a working gateway answers as they come back (Auth reached, the rate limit
   counted in Redis, a missing token refused at the edge); and asks the web app for its
   page and the script the page names. Every step is idempotent, so a failed run is re-run.
   No value is printed.

   ```powershell
   node infra/eks/up.mjs --context eventhub-demo
   kubectl --context eventhub-demo get pods,pvc
   ```

   **Or with Argo CD: `--gitops`**, which deploys main as CI published it, and keeps the
   cluster on main from then on. The installer still makes the credentials, every Secret
   and the stores; then it installs Argo CD (the version kind runs), hands it the
   repository through the read-only deploy key kind uses (`%USERPROFILE%\.eventhub\argocd-deploy-key`),
   and applies `infra/argocd/eks/root.json`. Every service, the gateway, the web app and
   setup are then Argo CD's, each on the images `publish.yml` pushed to GitHub's container
   registry for the commit it deploys. The checkout must be main itself, and CI must have
   published main's images.

   The registry is private, so the cluster pulls with a token of yours that can only read
   packages. Make it once: GitHub, Settings, Developer settings, Personal access tokens,
   Tokens (classic), Generate new token (classic), with an expiry and **only** the
   `read:packages` scope. Then, in the same PowerShell window, paste it where the prompt
   asks, so it is in no file and no history:

   ```powershell
   $env:GHCR_TOKEN = Read-Host -Prompt 'read:packages token'
   git switch main; git pull
   node infra/eks/up.mjs --context eventhub-demo --gitops
   ```

   `--down` removes Argo CD's Applications first (with self-heal on, Argo CD would put back
   anything taken off before it), then Argo CD, then the stores and their disks.

   The gateway and the web app are ClusterIP until the domain is decided. To open the app
   from this machine, stop compose's Kong first: the page calls the API on port 8000 of its
   own host, where compose's Kong otherwise answers. Each port-forward runs in its own
   terminal:

   ```powershell
   docker stop eventhub-kong
   kubectl --context eventhub-demo port-forward svc/kong 8000:8000
   kubectl --context eventhub-demo port-forward svc/web 5173:3000
   ```

   Then open `http://acme.eventhub.localhost:5173`. `docker start eventhub-kong` gives the
   local stack its gateway back.

   The Load Balancer Controller's chart, when the cluster gets a public way in, takes
   `clusterName` (`cluster_name`), `region`, `vpcId` (`vpc_id`), and a service account
   named `aws-load-balancer-controller` in `kube-system`, with **no** role annotation:
   Terraform's Pod Identity association binds `aws_load_balancer_controller_role_arn` to
   that name. Install the chart release that ships controller v3.6.3, the version the IAM
   policy is copied from.

## What an hour costs

ap-southeast-2 on-demand list prices, read from the AWS Price List API on 2026-11-13
(`aws pricing get-products --region us-east-1`, location "Asia Pacific (Sydney)"). The
public IPv4 rate and the KMS key price are AWS's published flat rates, not read from the API.

| What | ap-southeast-2 list price | Per hour |
|---|---|---|
| EKS control plane, standard support | $0.10 per cluster-hour (extended support would be $0.60; `upgrade_policy` keeps it standard) | $0.10 |
| 2 x m7i-flex.large, Linux, on demand | $0.1197 per instance-hour (t3.medium, $0.0528, is not one the free plan launches; `modules/eks/variables.tf` says why this one) | $0.239 |
| 1 NAT gateway | $0.059 per hour, plus $0.059 per GB it processes | $0.059, plus traffic |
| The NAT's public IPv4 address | $0.005 per hour | $0.005 |
| 2 x 20 GiB node root volumes (EBS gp3) | $0.096 per GB-month | about $0.005 |
| 4 x 10 GiB store disks (EBS gp3: Postgres, OpenSearch, ClickHouse, Kafka) | $0.096 per GB-month; the EBS CSI driver itself is free | about $0.005 |
| KMS key for Secrets | $1 per key-month, prorated | about $0.001 |
| ECR | $0.10 per GB-month stored; basic scan on push | pennies a month |
| State bucket and budget | kilobytes in S3; one budget is inside the allowance AWS publishes | about $0 |
| **Total** | | **about $0.41 an hour** |

A four-hour demo window is about $1.65, plus traffic. A forgotten weekend is about $25, past
the budget.

**On the free plan a forgotten environment does not become a bill: it closes the account.**
The credits pay for everything, and $120 of them is about 290 hours, twelve days. AWS's
words: "Account closes when credits are depleted or when the plan duration ends", and on
closing you lose access to the account's resources and data, the state bucket included.
AWS keeps it 90 days, and upgrading in AWS Settings within them restores access. The budget
counts usage before credits, so its emails come long before that. On the paid plan, a month
left running is about $300, thirty times the budget. Check the credits before each window:

```powershell
aws freetier get-account-plan-state --region ap-southeast-2 --profile eventhub --query "[accountPlanType,accountPlanRemainingCredits.amount,accountPlanExpirationDate]" --output text
```

Not in the table: load balancers the controller creates once an Ingress exists (billed per
hour plus usage), and cross-zone traffic. Image layers from ECR come from S3 through the
free gateway endpoint, not through the NAT; the stores' images come from Docker Hub and
do go through it, about 4 GB the first time on each node.

The budget is a backstop, not a timer. AWS refreshes cost data a few times a day, so an
alert can arrive hours after the spend. The runbook is the control.

## Teardown runbook

In this order. Terraform knows nothing about the load balancers the controller created, so
they go first, while the controller is still running to delete them.

```powershell
# The renewing profile: the destroy of the first demo took 19 minutes, past the login's 15.
$env:AWS_PROFILE = 'eventhub-tf'
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

3. **Delete the stores, and with them their disks, while the EBS CSI driver still runs.**
   Terraform made neither the claims nor the volumes behind them, and destroys the driver
   that could delete them: a volume whose claim outlives the driver stays in the account,
   unattached, billing. Each StatefulSet takes its claim with it
   (`persistentVolumeClaimRetentionPolicy`), and each claim its volume (reclaim `Delete`).
   Wait until both lists are empty, then ask AWS: every volume the driver made carries
   `ebs.csi.aws.com/cluster=true`, and none should be left.

   `--down` does it in that order -- the services and setup, then the stores -- and then
   waits for both lists to be empty and asks AWS, failing if anything is left:

   ```powershell
   node infra/eks/up.mjs --context eventhub-demo --down
   aws ec2 describe-volumes --region ap-southeast-2 --profile eventhub --filters "Name=tag:ebs.csi.aws.com/cluster,Values=true" --query "Volumes[].[VolumeId,State,Size]" --output table
   ```

4. **Destroy the demo environment.** Read the plan it shows: everything in it should be
   the demo's, and nothing from bootstrap.

   ```powershell
   terraform -chdir=infra/terraform/envs/demo destroy
   ```

5. **Look for what outlived it**: unattached EBS volumes, network interfaces, and
   addresses. A volume or interface tagged `kubernetes.io/cluster/eventhub-demo` is the
   cluster's leftover, and so is a volume tagged `ebs.csi.aws.com/cluster=true` or
   `Project=eventhub` that step 3 missed; delete it with `aws ec2 delete-volume --volume-id <id>` or
   `aws ec2 delete-network-interface --network-interface-id <id>` (same `--region` and
   `--profile`). An address with no association is billing for nothing.

   ```powershell
   aws ec2 describe-volumes --region ap-southeast-2 --profile eventhub --filters "Name=status,Values=available" --query "Volumes[].[VolumeId,Size,CreateTime]" --output table
   aws ec2 describe-network-interfaces --region ap-southeast-2 --profile eventhub --filters "Name=status,Values=available" --query "NetworkInterfaces[].[NetworkInterfaceId,VpcId,Description]" --output table
   aws ec2 describe-addresses --region ap-southeast-2 --profile eventhub --query "Addresses[].[PublicIp,AllocationId,AssociationId]" --output table
   aws logs describe-log-groups --region ap-southeast-2 --profile eventhub --log-group-name-prefix /aws/eks/eventhub-demo --query "logGroups[].logGroupName" --output table
   ```

   The KMS key is not deleted at once: `destroy` schedules it, seven days out.

6. **Keep the bootstrap bucket and budget.** They cost close to nothing, the budget keeps
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
