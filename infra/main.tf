# Required for setting up the project
data "google_billing_account" "account" {
  billing_account = var.billing_account
}


# Required provider. Make sure to run `terraform init` to set it up
provider "google" {
  project = local.project_id
  region  = var.region
  zone    = var.zone
}


# Google Cloud Project Setup #
resource "google_project" "my_project" {
  name            = local.project_id
  project_id      = local.project_id
  folder_id       = var.folder_id
  billing_account = data.google_billing_account.account.id
}
