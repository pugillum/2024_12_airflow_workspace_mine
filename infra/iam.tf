# IAM permissions

# Give bucket access to user service accounts
resource "google_project_iam_binding" "storage-admin" {
  project = local.project_id
  role    = "roles/storage.admin"
  members = local.service_accounts
}

# Give dataset access to user service accounts
resource "google_project_iam_binding" "editor" {
  project    = local.project_id
  role       = "roles/bigquery.dataEditor"
  members = local.service_accounts
}

# Allow users to run BigQuery jobs
resource "google_project_iam_binding" "jobUser" {
  project = local.project_id
  role    = "roles/bigquery.jobUser"
  members = local.service_accounts
}
