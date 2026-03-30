variable "project_id" {
  description = "GCP Project ID"
  type        = string
  default     = "de-zoomcamp-2026-485916"
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP zone"
  type        = string
  default     = "us-central1-a"
}

variable "credentials_file" {
  description = "Path to GCP credentials JSON"
  type        = string
  default     = "../gcp-credentials.json"
}