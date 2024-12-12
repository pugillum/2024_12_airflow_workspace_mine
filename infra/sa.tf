# Service Account Setup #
# Set up a service account for all users
resource "google_service_account" "airflow" {
  account_id = "airflow-user"
  depends_on = [google_project.my_project]
}

# Service Account Key for use within Airflow #
resource "google_service_account_key" "mykey" {
  service_account_id = google_service_account.airflow.name
  public_key_type    = "TYPE_X509_PEM_FILE"
}

# Export the key to a file, so it can be shared to Airflow users
# The connection can be added via Airflow > Admin > Connections > Create.
# Use the file contents in keyfile_json
# NOTE: This key only gets created once! Remove it from Google Cloud Console if you want to recreate it.
resource "local_file" "private_key" {
  content  = base64decode(google_service_account_key.mykey.private_key)
  filename = "google_service_account_key.json"
}
