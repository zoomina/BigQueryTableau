WITH base AS (
  SELECT
    LOWER(TRIM(trafficSource.medium))         AS medium,
    LOWER(TRIM(trafficSource.source))         AS src,
    LOWER(TRIM(hit.social.socialNetwork))  AS sn
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`,
       UNNEST(hits) AS hit
),
-- source에서 host 추출 -> eTLD+1
hosted AS (
  SELECT medium, src, sn,
         COALESCE(NULLIF(NET.HOST(src), ''), REGEXP_EXTRACT(src, r'^[a-z0-9]+')) AS host
  FROM base
),
-- host에서 brand 추출
parsed AS (
  SELECT medium, src, sn, host, 
         COALESCE(NET.REG_DOMAIN(host), host) AS reg_domain,
         SPLIT(COALESCE(NET.REG_DOMAIN(host), host), '.')[OFFSET(0)] AS brand
  FROM hosted
)

SELECT DISTINCT medium, src, sn, host, parsed.reg_domain, brand, 
       CASE
        WHEN sn IS NULL OR sn IN ('', '(not set)', '(none)', 'not set', 'na', 'none', 'n/a')
        THEN brand
        ELSE sn
       END AS final
FROM parsed