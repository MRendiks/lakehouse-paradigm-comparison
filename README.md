## Structure Projects

```
lakehouse-paradigm-comparison/
├── .gitignore
├── README.md
├── scripts/
│   ├── auth-setup.sh               # Automatic script for gcloud auth & impersonation
│   └── setup-minikube.sh           # Kafka installation script in local K8s
├── infrastructure/
│   └── gcp/
│       ├── providers.tf            # Google Provider & Impersonation Configuration
│       ├── main.tf                 # GCS Buckets Definition (Bronze, Silver, Gold)
│       ├── bigquery.tf             # BigQuery Dataset & Table Definition
│       ├── iam.tf                  # Role Binding Definition for Snowflake & Databricks
│       ├── variables.tf            # Terraform variables declaration
│       ├── terraform.tfvars        # Variable values (Project ID, Region, etc.)
│       └── outputs.tf              # GCS URI & BQ Dataset ID Outputs
├── ingestion/
│   ├── .env                        # Local env configuration (DO NOT commit)
│   ├── .env.example                # Configuration template for other teams
│   ├── Dockerfile                  # Containerization for Engine
│   ├── requirements.txt            # Python Libraries (pandas, google-cloud-storage, kafka-python)
│   ├── deploy/
│   │   └── k8s-deployment.yaml     # Manifest to deploy engine to Kubernetes
│   ├── tests/
│   │   ├── unit/                   # Unit testing for service logic
│   │   └── integration/            # Testing connection to Kafka/GCS
│   └── source/
│       ├── main.py                 # Application entry point
│       ├── controller/
│       │   └── ingestion_ctl.py    # Stream/batch flow orchestrator
│       ├── config/
│       │   └── settings.py         # Parse .env using Pydantic
│       ├── core/
│       │   ├── base_service.py     # Auth Logic (ADC) & GCS/Kafka Client init
│       │   ├── constants.py        # Enum for Layer (Bronze, Silver, Gold)
│       │   └── exceptions.py       # Custom Error Handling for Pipeline
│       ├── services/
│       │   ├── kafka_svc.py        # Kafka Producer & Consumer Logic
│       │   └── storage_svc.py      # GCS Objects Upload & List Logic
│       ├── mapper/
│       │   ├── configs/
│       │   │   └── ecommerce_map.json # Mapping source fields to target schema
│       │   └── handler/
│       │       └── transform_map.py # Initial data cleansing logic (Pre-processing)
│       ├── models/
│       │   └── schema_model.py     # Pydantic models for data contract validation
│       └── utils/
│           ├── logger.py           # Centralized logging
│           └── helper.py           # Helper functions (date formatter, string cleaner)
├── processing/
│   ├── databricks/
│   │   ├── bronze_to_silver.py     # Spark Job: Cleansing & Delta conversion
│   │   └── silver_to_gold.py       # Spark Job: Business Aggregation
│   └── notebooks/
│       └── exploration.ipynb       # EDA (Exploratory Data Analysis)
├── transformation/
│   └── dbt_project/
│       ├── dbt_project.yml         # dbt project configuration
│       ├── profiles.yml            # BQ & Snowflake connection configuration
│       ├── models/
│       │   ├── staging/            # SQL for initial cleaning
│       │   ├── intermediate/       # SQL for table joins
│       │   └── marts/              # SQL for final tables (Gold)
│       ├── tests/                  # dbt data tests
│       └── macros/                 # Reusable SQL functions
└── .github/
    └── workflows/
        ├── terraform-ci.yml        # CI for infrastructure validation
        └── python-app-ci.yml       # CI for linting & testing engine
```

## Getting Started

Follow these steps to set up and run the repository:

### 1. Prerequisites
- [Google Cloud SDK (gcloud)](https://cloud.google.com/sdk/docs/install)
- [Terraform](https://developer.hashicorp.com/terraform/downloads)
- [Python 3.10+](https://www.python.org/downloads/)
- [Minikube](https://minikube.sigs.k8s.io/docs/start/) (for local Kafka testing)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)

### 2. Authenticate with Google Cloud
Open your terminal and authenticate to your GCP account. This will provide Application Default Credentials (ADC) for both Terraform and Python.
```bash
gcloud auth login
gcloud auth application-default login
```

### 3. Provision Infrastructure
Navigate to the Terraform directory and initialize the GCP resources (GCS Buckets, BigQuery Datasets, and IAM Service Accounts).
```bash
cd infrastructure/gcp
terraform init
terraform plan
terraform apply
```
> **Note:** Ensure you have updated `project_id` in `infrastructure/gcp/terraform.tfvars` with your actual GCP Project ID.

### 4. Setup Python Environment
Navigate to the ingestion folder and install dependencies using `uv`.
```bash
cd ../../ingestion
# Install dependencies and create a virtual environment automatically
uv sync
# Activate the virtual environment
source .venv/bin/activate  # On Windows use: .venv\Scripts\activate
```

### 5. Setup Local Kafka (Minikube)
Run the setup script to deploy Kafka to your local Kubernetes cluster.
```bash
cd ../scripts
chmod +x setup-minikube.sh
./setup-minikube.sh
```

### 6. Run the Ingestion Engine
Configure your local environment variables and start the pipeline.
```bash
cp ../ingestion/.env.example ../ingestion/.env
# Update .env with your credentials and project settings

cd ../ingestion
# Run the pipeline orchestrator using uv
uv run ingestion-run producer batch --data-dir /path/to/olist/data --env dev

# Or run a single file ingestion
uv run ingestion-run producer single-file --filename olist_orders_dataset.csv --data-dir /path/to/olist/data
```

### 7. Run the Consumer (Kafka to GCS)
To consume data from Kafka and upload it to the GCS Bronze layer, run the consumer. 

**Pro Tip:** Add `GCS_BRONZE_BUCKET=your-bucket-name` to your `.env` file so you don't have to specify the `--bucket` flag manually!

You can use the `stream-all` command for local development to stream all topics concurrently:
```bash
uv run ingestion-run consumer-to-gcs stream-all --bucket <YOUR_GCS_BRONZE_BUCKET> --env dev
```

For production or targeted streaming, run the `stream` command per topic (useful for isolated Kubernetes Pods):
```bash
# Orders
uv run ingestion-run consumer-to-gcs stream --topic ecommerce.olist.orders.v1 --entity order --bucket <YOUR_GCS_BRONZE_BUCKET>

# Order Items
uv run ingestion-run consumer-to-gcs stream --topic ecommerce.olist.order-items.v1 --entity order_item --bucket <YOUR_GCS_BRONZE_BUCKET>

# Payments
uv run ingestion-run consumer-to-gcs stream --topic ecommerce.olist.payments.v1 --entity payment --bucket <YOUR_GCS_BRONZE_BUCKET>

# Reviews
uv run ingestion-run consumer-to-gcs stream --topic ecommerce.olist.reviews.v1 --entity review --bucket <YOUR_GCS_BRONZE_BUCKET>

# Customers
uv run ingestion-run consumer-to-gcs stream --topic ecommerce.olist.customers.v1 --entity customer --bucket <YOUR_GCS_BRONZE_BUCKET>

# Products
uv run ingestion-run consumer-to-gcs stream --topic ecommerce.olist.products.v1 --entity product --bucket <YOUR_GCS_BRONZE_BUCKET>

# Product Categories Translation
uv run ingestion-run consumer-to-gcs stream --topic ecommerce.olist.product-categories.v1 --entity product_category --bucket <YOUR_GCS_BRONZE_BUCKET>

# Sellers
uv run ingestion-run consumer-to-gcs stream --topic ecommerce.olist.sellers.v1 --entity seller --bucket <YOUR_GCS_BRONZE_BUCKET>

# Geolocation
uv run ingestion-run consumer-to-gcs stream --topic ecommerce.olist.geolocation.v1 --entity geolocation --bucket <YOUR_GCS_BRONZE_BUCKET>
```