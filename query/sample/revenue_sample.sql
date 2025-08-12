SELECT visitId,visitStartTime, hit.transaction.transactionId, p.v2ProductName, p.v2ProductCategory, p.productPrice, p.productQuantity, geoNetwork.country,
       CASE WHEN geoNetwork.region = "not available in demo dataset" THEN "" ELSE geoNetwork.region END AS region, 
       CASE WHEN geoNetwork.city = "not available in demo dataset" THEN "" ELSE geoNetwork.city END AS city
FROM  `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`,
      UNNEST(hits) AS hit,
      UNNEST(hit.product) AS p
WHERE transaction.transactionRevenue IS NOT NULL
ORDER BY visitId
-- LIMIT 100;