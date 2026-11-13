# TEN DOLLARS A MONTH, AND EMAIL BEFORE IT IS GONE.
#
# The demo environment costs roughly 28 cents an hour (infra/terraform/README.md), so a
# demo window is a dollar or two, and an environment left running by mistake is the
# whole budget in under two days. This is the backstop for that mistake, not a timer:
# AWS updates cost data a few times a day, so an alert can arrive hours after the spend.
# The teardown runbook is the control; this is what notices when it was not followed.
#
# No cost filter, on purpose. What keeps billing after a teardown is exactly what
# Terraform did not create and did not tag: a load balancer the controller made, an EBS
# volume a claim left behind. An account-wide budget sees those too.
resource "aws_budgets_budget" "monthly" {
  name         = "eventhub-monthly-cost"
  budget_type  = "COST"
  limit_amount = "10"
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  # USAGE BEFORE CREDITS. AWS Budgets nets credits off by default (IncludeCredit = true),
  # and this account runs on Free Tier credits: every dollar the demo spends is paid by a
  # credit, so a budget that counts them sees about $0 and never alerts -- while the $120
  # it would have warned about runs down. Counting usage gross is what makes the alert
  # fire. Found reading the first real plan, whose cost_types were unset.

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 50
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = var.budget_alert_emails
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = var.budget_alert_emails
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = var.budget_alert_emails
  }

  # Forecast, so the warning can come before the month's spend reaches the limit rather
  # than after.
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = var.budget_alert_emails
  }
}
