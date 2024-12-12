variable "billing_account" {
  type = string
}

variable "folder_id" {
  type = string
}

variable "project_prefix" {
  type = string
}

variable "training_date" {
  type = string
}

variable "user_emails" {
  type = list(string)
}

variable "region" {
    type = string
}

variable "zone" {
    type = string
}

locals {
  project_id = "${var.project_prefix}-${var.training_date}" # Make project id unique

  service_accounts = concat( # Add user email addresses to service account
    ["serviceAccount:${google_service_account.airflow.email}"],
    [for email in var.user_emails : "user:${email}"]
  )
}
