CREATE OR REPLACE TABLE `hardy-palace-468300-u5.mart.overview_productCategory`
PARTITION BY DATE_TRUNC(date, MONTH)
CLUSTER BY v2ProductCategory
OPTIONS (
  partition_expiration_days = NULL
)
AS
WITH hits_tx AS (
  SELECT PARSE_DATE('%Y%m%d', date) AS date,
    CONCAT(fullVisitorId, ':', CAST(visitId AS STRING)) AS session_key,
    p.v2ProductCategory,
    hit.transaction.transactionId AS tx_id,
    hit.transaction.transactionRevenue AS tx_rev,
    ROW_NUMBER() OVER (PARTITION BY hit.transaction.transactionId) AS rn
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
       UNNEST(hits) AS hit,
       UNNEST(hit.product) AS p
  WHERE hit.transaction.transactionRevenue IS NOT NULL
        AND _TABLE_SUFFIX BETWEEN '20170501' AND '20170731'
)

SELECT date, v2ProductCategory,
  SAFE_DIVIDE(SUM(IF(rn = 1, tx_rev, 0)), 1000000) AS revenue
FROM hits_tx
GROUP BY date, v2ProductCategory