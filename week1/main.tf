# Terraform configuration
terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

# Provider configuration
provider "google" {
  project     = var.project_id
  region      = var.region
  credentials = file(var.credentials_file)
}

# Google Cloud Storage Bucket
resource "google_storage_bucket" "data_lake_bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  storage_class = var.gcs_storage_class

  # Optional: Prevent accidental deletion
  force_destroy = true

  # Uniform bucket-level access
  uniform_bucket_level_access = true

  # Versioning
  versioning {
    enabled = true
  }

  # Lifecycle rules (optional)
  lifecycle_rule {
    condition {
      age = 30 # Days
    }
    action {
      type = "Delete"
    }
  }
}

# BigQuery Dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id    = var.bq_dataset_name
  friendly_name = "NY Taxi Data"
  description   = "Dataset for NY Taxi trip data from Data Engineering Zoomcamp"
  location      = var.location

  # Delete contents when dataset is deleted
  delete_contents_on_destroy = true

  # Optional: Set default table expiration (in milliseconds)
  # default_table_expiration_ms = 3600000  # 1 hour
}

# Output the bucket name
output "gcs_bucket_name" {
  description = "Name of the GCS bucket"
  value       = google_storage_bucket.data_lake_bucket.name
}

# Output the bucket URL
output "gcs_bucket_url" {
  description = "URL of the GCS bucket"
  value       = google_storage_bucket.data_lake_bucket.url
}

# Output the BigQuery dataset ID
output "bq_dataset_id" {
  description = "BigQuery Dataset ID"
  value       = google_bigquery_dataset.dataset.dataset_id
}

# Output the BigQuery dataset location
output "bq_dataset_location" {
  description = "BigQuery Dataset Location"
  value       = google_bigquery_dataset.dataset.location
}