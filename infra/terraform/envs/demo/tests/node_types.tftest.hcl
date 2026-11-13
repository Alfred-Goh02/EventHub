# THE FREE-PLAN NODE GUARD, TESTED BY WHAT A PLAN DOES. verify-terraform.mjs reads the
# files, and a file can name both variables in a precondition that can never fail: five
# such mutations left every static guard green. These runs plan, and watch.
#
# The provider is mocked: no account, no credentials, nothing created. What a real plan
# reads from describe-instance-types is given by override_data, as AWS answered it in
# ap-southeast-2 on 2026-11-13. verify-terraform.mjs runs this after init:
#   terraform -chdir=infra/terraform/envs/demo test
#
# Two halves, because a test may expect failures only from objects in the module it runs:
#   - envs/demo: the list read from AWS reaches modules/eks, and only on the free plan;
#   - modules/eks, run on its own: given that list, the precondition refuses a type outside it.

# A mock invents every value it is not given. Three it invents would fail the plan before
# the guard is reached: a policy document that is not JSON (IAM roles refuse it), no
# availability zones (modules/network takes two), and a partition no ARN has.
mock_provider "aws" {
  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }

  mock_data "aws_availability_zones" {
    defaults = {
      names = ["ap-southeast-2a", "ap-southeast-2b", "ap-southeast-2c"]
    }
  }

  mock_data "aws_partition" {
    defaults = {
      partition = "aws"
    }
  }
}

# ---------------------------------------------------------------------------
# envs/demo: the wiring
# ---------------------------------------------------------------------------

run "on_the_free_plan_by_default_the_list_from_aws_reaches_the_node_group" {
  command = plan

  variables {
    eks_public_access_cidrs = ["203.0.113.10/32"]
    admin_principal_arn     = "arn:aws:iam::123456789012:role/eventhub-admin"
  }

  override_data {
    target = data.aws_ec2_instance_types.launchable_on_free_plan
    values = {
      instance_types = ["c7i-flex.large", "m7i-flex.large", "t3.micro", "t3.small", "t4g.micro", "t4g.small", "t8i.micro", "t8i.small"]
    }
  }

  assert {
    condition     = var.free_plan == true
    error_message = "free_plan must default to true: the account is on the free plan, and a wrong false fails the way the first apply did."
  }

  assert {
    condition     = toset(coalesce(module.eks.allowed_instance_types, [])) == toset(["c7i-flex.large", "m7i-flex.large", "t3.micro", "t3.small", "t4g.micro", "t4g.small", "t8i.micro", "t8i.small"])
    error_message = "modules/eks must be given the list describe-instance-types answered, so its precondition has something to check against."
  }
}

run "after_an_upgrade_nothing_is_checked" {
  command = plan

  variables {
    eks_public_access_cidrs = ["203.0.113.10/32"]
    admin_principal_arn     = "arn:aws:iam::123456789012:role/eventhub-admin"
    free_plan               = false
    node_instance_types     = ["t3.medium"]
  }

  assert {
    condition     = module.eks.allowed_instance_types == null
    error_message = "With free_plan = false the node group must not be checked against the free plan's list."
  }
}

# ---------------------------------------------------------------------------
# modules/eks, on its own: the precondition
# ---------------------------------------------------------------------------

run "a_type_the_account_cannot_launch_is_refused_at_plan_time" {
  command = plan

  module {
    source = "../../modules/eks"
  }

  variables {
    cluster_name           = "test"
    subnet_ids             = ["subnet-a", "subnet-b"]
    public_access_cidrs    = ["203.0.113.10/32"]
    admin_principal_arn    = "arn:aws:iam::123456789012:role/eventhub-admin"
    node_instance_types    = ["t3.medium"]
    allowed_instance_types = ["c7i-flex.large", "m7i-flex.large", "t3.small"]
  }

  expect_failures = [aws_eks_node_group.default]
}

run "one_bad_type_beside_a_good_one_is_still_refused" {
  command = plan

  module {
    source = "../../modules/eks"
  }

  variables {
    cluster_name           = "test"
    subnet_ids             = ["subnet-a", "subnet-b"]
    public_access_cidrs    = ["203.0.113.10/32"]
    admin_principal_arn    = "arn:aws:iam::123456789012:role/eventhub-admin"
    node_instance_types    = ["m7i-flex.large", "t3.medium"]
    allowed_instance_types = ["c7i-flex.large", "m7i-flex.large", "t3.small"]
  }

  expect_failures = [aws_eks_node_group.default]
}

run "an_empty_list_refuses_everything" {
  command = plan

  module {
    source = "../../modules/eks"
  }

  variables {
    cluster_name           = "test"
    subnet_ids             = ["subnet-a", "subnet-b"]
    public_access_cidrs    = ["203.0.113.10/32"]
    admin_principal_arn    = "arn:aws:iam::123456789012:role/eventhub-admin"
    allowed_instance_types = []
  }

  expect_failures = [aws_eks_node_group.default]
}

# The control for the three above: the same module, the same list, a type in it, plans.
# Without this, a precondition that refused everything would pass them all.
run "the_default_type_is_one_the_free_plan_launches" {
  command = plan

  module {
    source = "../../modules/eks"
  }

  variables {
    cluster_name           = "test"
    subnet_ids             = ["subnet-a", "subnet-b"]
    public_access_cidrs    = ["203.0.113.10/32"]
    admin_principal_arn    = "arn:aws:iam::123456789012:role/eventhub-admin"
    allowed_instance_types = ["c7i-flex.large", "m7i-flex.large", "t3.small"]
  }
}

run "with_no_list_any_type_plans" {
  command = plan

  module {
    source = "../../modules/eks"
  }

  variables {
    cluster_name        = "test"
    subnet_ids          = ["subnet-a", "subnet-b"]
    public_access_cidrs = ["203.0.113.10/32"]
    admin_principal_arn = "arn:aws:iam::123456789012:role/eventhub-admin"
    node_instance_types = ["t3.medium"]
  }
}
