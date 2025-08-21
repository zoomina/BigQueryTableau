-- 월 파티션 + 클러스터(선택) + 3개월 범위
CREATE OR REPLACE TABLE `hardy-palace-468300-u5.mart.overview_traffic`
PARTITION BY DATE_TRUNC(date, MONTH)
CLUSTER BY device, medium
OPTIONS (
  partition_expiration_days = NULL
)
AS
SELECT
  PARSE_DATE('%Y%m%d', t.date) AS date,
  t.device,
  t.medium,

  COUNT(DISTINCT t.session_key) AS sessions,

  COUNT(DISTINCT IF(t.is_bounce = 1,     t.session_key, NULL)) AS bounces,
  COUNT(DISTINCT IF(t.is_new_visit = 1,  t.session_key, NULL)) AS new_visits,

  COUNT(DISTINCT IF(t.action_type = "2", t.session_key, NULL)) AS detail_view,
  COUNT(DISTINCT IF(t.action_type = "3", t.session_key, NULL)) AS add2cart,
  COUNT(DISTINCT IF(t.action_type = "6", t.session_key, NULL)) AS purchase

FROM (
  SELECT
    date,
    CONCAT(CAST(fullVisitorId AS STRING), "-", CAST(visitId AS STRING)) AS session_key,

    -- 버킷팅(간단 CASE로 인라인)
    CASE
      WHEN device.operatingSystem = "(not set)" OR device.deviceCategory = "(not set)" THEN "others"
      WHEN device.deviceCategory = "mobile" THEN device.operatingSystem
      ELSE device.deviceCategory
    END AS device,
    CASE
      WHEN trafficSource.medium = "(none)" THEN "direct"
      ELSE trafficSource.medium
    END AS medium,

    totals.bounces    AS is_bounce,     -- 세션 레벨 플래그
    totals.newVisits  AS is_new_visit,  -- 세션 레벨 플래그

    hit.eCommerceAction.action_type AS action_type
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
       UNNEST(hits) AS hit,
       UNNEST(hit.product) AS p
WHERE _TABLE_SUFFIX BETWEEN '20170501' AND '20170731'
) AS t
GROUP BY t.date, t.device, t.medium;