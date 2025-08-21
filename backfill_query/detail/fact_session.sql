CREATE SCHEMA IF NOT EXISTS `hardy-palace-468300-u5.detail`
OPTIONS(
  location="US"
);

-- UDF
CREATE TEMP FUNCTION channel_src_final(medium STRING, src STRING, sn STRING)
RETURNS STRING AS ((
  WITH host AS (
    SELECT COALESCE(NULLIF(NET.HOST(src), ''), REGEXP_EXTRACT(src, r'^[a-z0-9]+')) AS h
  )
  SELECT CASE
           WHEN sn IS NULL OR sn IN ('', '(not set)', '(none)', 'not set', 'na', 'none', 'n/a')
             THEN SPLIT(COALESCE(NET.REG_DOMAIN(h), h), '.')[OFFSET(0)]
           ELSE sn
         END
  FROM host
));

CREATE TEMP FUNCTION channel_farm_key(medium STRING, src STRING, sn STRING)
RETURNS INT64 AS (FARM_FINGERPRINT(medium || '|' || channel_src_final(medium, src, sn)));

CREATE TEMP FUNCTION geo_farm_key(country STRING, region STRING)
RETURNS INT64 AS (FARM_FINGERPRINT(country || '|' || COALESCE(region, '')));

-- session staging
CREATE TEMP TABLE sessions_stage AS
SELECT
  s.visitId, s.fullVisitorId, s.visitStartTime, s.date,
  s.totals.newVisits AS newVisits,
  s.totals.bounces   AS bounces,
  CASE WHEN s.geoNetwork.country IN ('United States','Canada') THEN s.geoNetwork.country ELSE 'Others' END AS region_group,
  s.geoNetwork.country,
  CASE
    WHEN s.geoNetwork.region IN ('not available in demo dataset', '(not set)') THEN NULL
    WHEN s.geoNetwork.country IN ('United States','Canada') THEN s.geoNetwork.region
    ELSE NULL
  END AS region,
  CASE
    WHEN s.device.operatingSystem = "(not set)" THEN "others"
    WHEN s.device.deviceCategory  = "(not set)" THEN "others"
    WHEN s.device.deviceCategory  = "mobile"    THEN s.device.operatingSystem
    ELSE s.device.deviceCategory
  END AS device,
  LOWER(TRIM(s.trafficSource.medium)) AS medium,
  LOWER(TRIM(s.trafficSource.source)) AS src,
  (
    SELECT LOWER(TRIM(h.social.socialNetwork))
    FROM UNNEST(s.hits) h
    WHERE h.social.socialNetwork IS NOT NULL
    LIMIT 1
  ) AS sn
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` s
WHERE _TABLE_SUFFIX BETWEEN '20170501' AND '20170731';

-- channel staging
CREATE TEMP TABLE channel_stage AS
SELECT DISTINCT
  medium, src, sn,
  channel_src_final(medium, src, sn) AS src_final,
  channel_farm_key(medium, src, sn)  AS channel_key
FROM sessions_stage;

----------------------------------------------------------------
-- fact_session
-- - grain : visitId
-- - partition : date
-- - Key/Label : visitId, fullVisitorId, date, PK(geo, channel)
-- - Data : pageviews, bounce, newVisit
----------------------------------------------------------------

CREATE OR REPLACE TABLE `hardy-palace-468300-u5.detail.fact_session`
PARTITION BY date
CLUSTER BY geo_key, channel_key, device AS
SELECT
  PARSE_DATE('%Y%m%d', s.date) AS date,
  CONCAT(CAST(s.fullVisitorId AS STRING), '-', CAST(s.visitId AS STRING)) AS session_id,
  geo_farm_key(s.country, s.region) AS geo_key,
  c.channel_key,
  s.device,
  s.bounces,
  s.newVisits
FROM sessions_stage s
LEFT JOIN channel_stage c
  ON c.medium = s.medium AND c.src = s.src AND c.sn = s.sn;

----------------------------------------------------------------
-- meta
-- - Geo : Country, Region(State), City
-- - Channel : medium, source
----------------------------------------------------------------

-- meta_geo
CREATE OR REPLACE TABLE `hardy-palace-468300-u5.detail.meta_geo` AS
SELECT DISTINCT
  geo_farm_key(country, region) AS geo_key,
  region_group, country, region
FROM sessions_stage;

-- meta_channel
CREATE OR REPLACE TABLE `hardy-palace-468300-u5.detail.meta_channel` AS
SELECT DISTINCT
  channel_key,
  medium,
  src_final AS source
FROM channel_stage;