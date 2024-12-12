# Google Cloud Storage

# Create bucket
resource "google_storage_bucket" "default" {
  name     = local.project_id
  project  = local.project_id
  location = "EU"
  depends_on = [google_project.my_project]
}
