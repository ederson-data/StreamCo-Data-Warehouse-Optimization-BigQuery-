# StreamCo-Data-Warehouse-Optimization-BigQuery-
Executive Summary StreamCo, a streaming platform with 50 million users, faced escalating Google Cloud costs and slow query performance due to a fragmented data architecture. I architected a production-ready data warehouse using BigQuery that reduced query costs by over 90% and improved performance by 10x.
The Challenge
The original dataset consisted of hundreds of daily "sharded" tables. This required the analytics team to scan gigabytes of data for simple monthly reports, leading to:

High Costs: Full table scans on every query.

Slow Dashboards: Aggregations calculated on the fly for every user.

Complexity: Nested GA4 structures (JSON-style) difficult for standard SQL users.

Technical Implementation
1. Storage Optimization (Partitioning & Clustering)

Date Partitioning: Consolidated daily shards into a single table partitioned by event_date. This allows Partition Pruning, where BigQuery ignores data outside the requested timeframe.

Clustering: Sorted data within partitions by event_name and city. This optimized high-frequency filter patterns for the marketing team.

2. Business Intelligence Layer

Standard Views: Created a semantic layer to flatten complex STRUCT and ARRAY fields, making data accessible for BI Tools like Power BI or Tableau.

Materialized Views: Pre-computed expensive revenue aggregations. By using Materialized Views, queries that previously took seconds now return results in milliseconds at zero additional cost for cached results.

3. Data Governance

Lifecycle Management: Implemented table expiration policies (7-day retention) for staging tables to minimize storage bloat.

Quality Assurance: Integrated ASSERT statements into the ETL pipeline to automatically flag negative revenue or null critical IDs before they reach the dashboard.
