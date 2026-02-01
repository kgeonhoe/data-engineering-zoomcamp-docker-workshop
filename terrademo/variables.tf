
variable "credentials" {
  description = "Path to GCP credentials JSON file"
  default     = "./keys/my-creds.json"
}

variable "project" {
  description = "Project ID"
  default     = "nimble-courier-485614-g9"
}
variable "region" {
  description = "region Location"
  default     = "us-central1"
}
variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "ny_taxi_dataset"
}

variable "gcs_bucket_name" {
  description = "Bucket Storage Class"
  default = "nimble-courier-485614-g9-data-lake"

}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default = "STANDARD"

}