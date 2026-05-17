terraform {
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.99"
    }
    # Google provider untuk otomatisasi IAM handshake GCS ↔ Snowflake
    google = {
      source  = "hashicorp/google"
      version = "~> 7.32.0"
    }
  }
}

# Auth Snowflake via username + password
# Untuk production: gunakan RSA key pair (lebih aman, tanpa password)
provider "snowflake" {
  organization_name = var.snowflake_organization
  account_name      = var.snowflake_account
  user              = var.snowflake_user
  password          = var.snowflake_password
  role              = "ACCOUNTADMIN"
}

# Google provider untuk memberi IAM access ke Snowflake SA di GCS bucket
provider "google" {
  project = var.gcp_project_id
}
