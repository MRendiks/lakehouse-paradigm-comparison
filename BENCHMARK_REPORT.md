# 📊 Lakehouse Benchmark & Cost Analysis Report

This report presents performance metrics and cost comparisons between **Google BigQuery** and **Snowflake** operating over the exact same Olist E-Commerce Medallion Gold Layer dataset on Google Cloud Storage.

## 1. Performance Comparisons

| Query Scenario | Platform | Cold Run (sec) | Warm Run (sec) | Data Scanned (MB) |
|---|---|---|---|---|
| **fct_orders_full_scan** | BigQuery | 0.772s | 0.447s | 10.00 MB |
| | Snowflake | 0.774s | 0.363s | *N/A* |
| **group_by_aggregation** | BigQuery | 0.398s | 0.383s | 10.00 MB |
| | Snowflake | 0.455s | 0.423s | *N/A* |
| **multi_join_enrichment** | BigQuery | 1.580s | 1.537s | 20.00 MB |
| | Snowflake | 0.537s | 0.510s | *N/A* |

## 2. Key Architecture Takeaways

### 🚀 Cold Run vs. Warm Run Performance
- **BigQuery:** Leverages massive serverless execution engines. Warm runs that hit the BigQuery Query Cache return in **0.00s to 0.10s** at zero scan cost. Cold runs are fast but bound by regional metadata parsing of GCS Delta Lake objects.
- **Snowflake:** Uses virtual warehouses. When `USE_CACHED_RESULT = TRUE` is enabled, warm runs skip compute entirely and load results from the Snowflake Cloud Services layer instantly. When computing, Snowflake leverages local warehouse SSD caches for repeat queries.

### 💸 Cost Models Analysis
- **BigQuery (On-Demand):** Costs are strictly based on bytes scanned (**$5.00 per TB**). For small/medium datasets like Olist, BigQuery is **extremely cost-effective** (effectively costing $0.0001 per query) because idle time is free.
- **Snowflake (Compute-Based):** Costs are based on warehouse runtime (**1 credit/hour for X-Small**; standard rate ~$3.00/hour). Minimum billing is 1 minute (60 seconds) every time the warehouse boots, making Snowflake more expensive for ad-hoc, low-volume query workloads unless warehouses are kept active for continuous pipelines.

## 3. Feature Matrix

| Capability | Google BigQuery | Snowflake |
|---|---|---|
| **Delta Lake Integration** | Native (directly parses schema & logs from GCS) | Requires External Stage + External Table mapping |
| **Compute Model** | Fully Serverless (automatic concurrency scaling) | Virtual Warehouse Clusters (multi-cluster auto-scaling) |
| **Pricing Model** | $5.00/TB scanned or Slots Capacity | ~$2.00 - $4.00 per Credit (Warehouse uptime) |
| **Query Caching** | 24-Hour Free Query Cache | Cloud Services Metadata Cache + Warehouse SSD cache |
| **Storage Type** | Internal Managed or GCS External | Internal Managed or Cloud External Stages |


*Report generated automatically on: 2026-05-17T23:41:48Z*
