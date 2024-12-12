# Airflow Training Exercises Infrastructure

This repository is used for setting up cloud infrastructure for the Airflow Training exercises. It uses Terraform to set up multiple components on Google Cloud.

# Required

- Install the gcloud CLI
- Create local authenitcation credentials using `gcloud auth application-default login` 

# Usage

Get into the infra folder:

```shell
cd infra
```

Initialize Terraform:

```shell
terraform init
```

See your changes:

```shell
make plan
```

Apply your changes:

```shell
make apply
```

# Adding users

Users can be added inside `main.tfvars`. Make sure to use the email addresses of their Google account.
You can modify the following list. On deployment, these users will be given access to the right resources.

```hcl
user_emails = [
  "timo.uelen@xebia.com",
  "ismael.cabral@xebia.com",
]
```

They will be able to access resources at [https://console.cloud.google.com](https://console.cloud.google.com).

# Adding the Google Service Account Key to Airflow

A service account key will stored in a file `google_service_account_key.json`. This file can be used in the Airflow interface. Go to Airflow > Admin > Connections and add a new connection, and use the following settings:

- Connection Id: `google_cloud_default`
- Connection Type: `Google Cloud`
- Keyfile JSON: <Copy and paste the json from the file here>

First test, then save the connection. Now, you can use the connection from inside Airflow.

# After the training, destroy the infrastructure

```shell
make destroy
```
