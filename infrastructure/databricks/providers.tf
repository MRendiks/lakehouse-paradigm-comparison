terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.49.0"
    }
  }
}

# Auth via Personal Access Token (PAT)
# Token dibaca dari terraform.tfvars — tidak pernah di-commit ke Git
provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}
