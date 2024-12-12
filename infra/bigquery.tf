# Google Cloud BigQuery

# Enable BigQuery storage backend
resource "google_project_service" "bigquery_storage" {
  project                    = local.project_id
  service                    = "bigquerystorage.googleapis.com"
  disable_dependent_services = true
  depends_on = [google_project.my_project]
}

# Enable BigQuery API
resource "google_project_service" "bq" {
  project                    = local.project_id
  service                    = "bigquery.googleapis.com"
  depends_on                 = [google_project_service.bigquery_storage]
  disable_dependent_services = true
}

# Create bigquery dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id = replace(local.project_id, "-", "_")
  project    = local.project_id
  depends_on = [google_project.my_project, google_project_service.bq]
}
