variable "project_id" {
  description = "de-zoomcamp-2026-485916"
  type        = string
  default     = "de-zoomcamp-2026-485916" # Replace with your actual project ID
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "location" {
  description = "Location for resources (for multi-region resources)"
  type        = string
  default     = "US"
}

variable "credentials_file" {
  description = "Path to GCP credentials JSON file"
  type        = string
  default     = "./gcp-credentials.json"
}

variable "gcs_bucket_name" {
  description = "Name of the GCS bucket (must be globally unique)"
  type        = string
  default     = "de-zoomcamp-2026-485916" # Change to make it unique
}

variable "bq_dataset_name" {
  description = "Name of the BigQuery dataset"
  type        = string
  default     = "ny_taxi_data"
}

variable "gcs_storage_class" {
  description = "Storage class for GCS bucket"
  type        = string
  default     = "STANDARD"
}