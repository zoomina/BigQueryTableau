-- 월 파티션 + 클러스터(선택) + 3개월 범위
CREATE OR REPLACE TABLE `hardy-palace-468300-u5.mart.overview_geo`
PARTITION BY DATE_TRUNC(date, MONTH)
CLUSTER BY country, region
OPTIONS (
  partition_expiration_days = NULL
)
AS
WITH hits_tx AS (
  SELECT
    PARSE_DATE('%Y%m%d', date) AS date,
    CONCAT(fullVisitorId, ':', CAST(visitId AS STRING)) AS session_key,
    geoNetwork.country,
    CASE
      WHEN geoNetwork.region IN ('not available in demo dataset', '(not set)') THEN NULL
      WHEN geoNetwork.country IN ('United States','Canada') THEN geoNetwork.region
      ELSE NULL
    END AS region,
    hit.transaction.transactionId AS tx_id,
    hit.transaction.transactionRevenue AS tx_rev,
    ROW_NUMBER() OVER (PARTITION BY hit.transaction.transactionId) AS rn
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
       UNNEST(hits) AS hit
  WHERE _TABLE_SUFFIX BETWEEN '20170501' AND '20170731'
    AND hit.transaction.transactionId IS NOT NULL
)
SELECT
  date,
  country,
  region,
  COUNT(DISTINCT session_key) AS sessions,
  SUM(IF(rn = 1, tx_rev, 0)) AS revenue
FROM hits_tx
GROUP BY date, country, region;
