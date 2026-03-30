terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials_file)
  project     = var.project_id
  region      = var.region
}

# ─────────────────────────────────────────
# GCS BUCKET — Data Lake
# Stores OHLCV Parquet exports from DuckDB
# ─────────────────────────────────────────
resource "google_storage_bucket" "crypto_data_lake" {
  name          = "${var.project_id}-crypto-data-lake"
  location      = var.region
  force_destroy = true

  lifecycle_rule {
    condition { age = 90 }
    action    { type = "Delete" }
  }

  labels = {
    project     = "crypto-streaming-pipeline"
    environment = "dev"
    managed_by  = "terraform"
  }
}

# Folder structure inside bucket
resource "google_storage_bucket_object" "raw_folder" {
  name    = "raw/.keep"
  bucket  = google_storage_bucket.crypto_data_lake.name
  content = "raw crypto tick data"
}

resource "google_storage_bucket_object" "ohlcv_folder" {
  name    = "ohlcv/.keep"
  bucket  = google_storage_bucket.crypto_data_lake.name
  content = "1-minute OHLCV candles"
}

resource "google_storage_bucket_object" "anomalies_folder" {
  name    = "anomalies/.keep"
  bucket  = google_storage_bucket.crypto_data_lake.name
  content = "detected anomalies"
}

# ─────────────────────────────────────────
# COMPUTE ENGINE VM — Pipeline host
# e2-micro = always free tier eligible
# ─────────────────────────────────────────
resource "google_compute_instance" "pipeline_vm" {
  name         = "crypto-pipeline-vm"
  machine_type = "e2-medium"
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
      size  = 30
    }
  }

  network_interface {
    network = "default"
    access_config {}
  }

  metadata_startup_script = <<-EOF
    #!/bin/bash
    apt-get update -y
    apt-get install -y docker.io docker-compose git curl

    systemctl start docker
    systemctl enable docker
    usermod -aG docker debian

    echo "Crypto Streaming Pipeline VM ready" > /tmp/startup_complete.txt
  EOF

  tags = ["crypto-pipeline", "http-server", "https-server"]

  labels = {
    project    = "crypto-streaming-pipeline"
    managed_by = "terraform"
  }
}

# ─────────────────────────────────────────
# FIREWALL RULES
# ─────────────────────────────────────────
resource "google_compute_firewall" "allow_pipeline_ports" {
  name    = "allow-crypto-pipeline-ports"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["22", "3000", "9090", "8080", "8085", "8083", "8086", "9092"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["crypto-pipeline"]

  description = "Allow access to crypto pipeline services"
}

# ─────────────────────────────────────────
# OUTPUTS
# ─────────────────────────────────────────
output "vm_external_ip" {
  description = "External IP of the pipeline VM"
  value       = google_compute_instance.pipeline_vm.network_interface[0].access_config[0].nat_ip
}

output "data_lake_bucket" {
  description = "GCS data lake bucket name"
  value       = google_storage_bucket.crypto_data_lake.name
}

output "vm_name" {
  description = "Name of the compute instance"
  value       = google_compute_instance.pipeline_vm.name
}

output "grafana_url" {
  description = "Grafana dashboard URL (once pipeline deployed)"
  value       = "http://${google_compute_instance.pipeline_vm.network_interface[0].access_config[0].nat_ip}:3000"
}