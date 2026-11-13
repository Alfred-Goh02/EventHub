variable "region" {
  description = "Where the state bucket lives. envs/*/versions.tf names the same region in its backend block."
  type        = string
  default     = "ap-southeast-2"
}

variable "budget_alert_emails" {
  description = "Who AWS Budgets emails when the month's cost passes 50%, 80% and 100% of the budget, or is forecast to pass 100%."
  type        = list(string)

  validation {
    condition     = length(var.budget_alert_emails) > 0 && alltrue([for e in var.budget_alert_emails : can(regex("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", e))])
    error_message = "budget_alert_emails needs at least one email address."
  }
}
