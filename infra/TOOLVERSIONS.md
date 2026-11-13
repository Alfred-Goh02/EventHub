# Infrastructure toolchain

What the infrastructure in this directory was built and checked against, on Windows 11,
installed with winget on 2026-11-13. Infra tools break across minor versions more often
than application libraries do; when something here stops working, compare against this
first.

| Tool | Version | Installed with | Why it matters |
|---|---|---|---|
| Terraform | 1.17.1 | `winget install -e --id Hashicorp.Terraform` | S3-native state locking (`use_lockfile`) needs 1.10 or newer |
| Helm | 4.3.1 | `winget install -e --id Helm.Helm` | the charts in `charts/` are `apiVersion: v2` |
| kind | 0.33.0 | `winget install -e --id Kubernetes.kind` | its default node image runs Kubernetes 1.37.0 |
| kubectl | 1.37 (pending) | `winget install -e --id Kubernetes.kubectl` | keep within one minor version of the cluster; Docker Desktop's bundled 1.34.1 works with a skew warning until then |
| AWS CLI | v2 (pending) | `winget install -e --id Amazon.AWSCLI` | needed from the Terraform part on; installs machine-wide, so Windows asks for admin |

Open a new terminal after installing: winget's PATH changes do not reach a shell that
was already running.

## The local cluster

`infra/local/kind-cluster.yaml` is one control plane and two workers. The services run in
it; the data stores stay in compose, as RDS and MSK will sit outside EKS.

```powershell
docker compose up -d                 # the data stores, and the image the charts run
node infra/local/up.mjs              # cluster, bridges to compose, keys, image, every chart
node infra/local/up.mjs auth event   # or just some
```

To try a branch without touching the image compose runs, build it under its commit and
name the tag:

```powershell
docker build -t eventhub/app:$(git rev-parse --short HEAD) .
$env:EVENTHUB_IMAGE_TAG = (git rev-parse --short HEAD); node infra/local/up.mjs
```

`kind delete cluster --name eventhub` removes it all; compose is untouched.
