-- Schema Discovery
SELECT
column_name, data_type 
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'events_20210131';


-- Documnting Baseline Costs
SELECT 
event_name,
SUM(ecommerce.purchase_revenue) AS revenue
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20201101' AND '20201130'
GROUP BY 1;

-- Creat Date Partitioned Table
CREATE OR REPLACE TABLE `composite-rhino-485716-n8.streamco_ops.events_partitioned`
PARTITION BY event_date_formatted
AS
SELECT 
  PARSE_DATE('%Y%m%d', event_date) AS event_date_formatted,
  * EXCEPT(event_date)
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`;


-- Verify Pruning with Explain
SELECT count(*)
FROM `composite-rhino-485716-n8.streamco_ops.events_partitioned`
WHERE event_date_formatted = '2020-12-01';


-- Create Clustered Table
CREATE OR REPLACE TABLE `composite-rhino-485716-n8.streamco_ops.events_optimized`
PARTITION BY event_date_formatted
CLUSTER BY event_name, city 
AS
SELECT 
  *,
  geo.city AS city
FROM `composite-rhino-485716-n8.streamco_ops.events_partitioned`;


-- Testing Filter Performance
SELECT 
  geo.city, 
  count(*) as page_views
FROM `composite-rhino-485716-n8.streamco_ops.events_optimized`
WHERE event_name = 'page_view'
GROUP BY 1 
ORDER BY 2 DESC;


-- Create a standard view
CREATE OR REPLACE VIEW `composite-rhino-485716-n8.streamco_ops.v_daily_metrics` AS
SELECT
  event_date_formatted,
  event_name,
  city,
  user_pseudo_id,
  COALESCE(event_value_in_usd, 0) AS revenue
FROM `composite-rhino-485716-n8.streamco_ops.events_optimized`;

-- Materialized view and Refresh Strategy
CREATE OR REPLACE MATERIALIZED VIEW `composite-rhino-485716-n8.streamco_ops.mv_daily_revenue_summary`
OPTIONS (enable_refresh = true, refresh_interval_minutes = 60) AS
SELECT
  event_date_formatted,
  event_name,
  SUM(COALESCE(event_value_in_usd, 0)) AS daily_total_revenue,
  COUNT(user_pseudo_id) AS total_event_count
FROM `composite-rhino-485716-n8.streamco_ops.events_optimized`
GROUP BY 1, 2;


-- Quality Check: Ensure we don't have negative revenue
ASSERT (
  SELECT COUNT(*)
  FROM `composite-rhino-485716-n8.streamco_ops.events_optimized` 
  WHERE event_value_in_usd < 0
) = 0 
AS "Data Quality Error: Negative revenue values detected!";



-- The performance Report
SELECT * FROM `composite-rhino-485716-n8.streamco_ops.mv_daily_revenue_summary`
WHERE event_date_formatted = '2020-12-01'
  AND event_name = 'purchase';