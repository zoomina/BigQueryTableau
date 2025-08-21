SELECT DISTINCT(visitId),visitStartTime, trafficSource.adContent, trafficSource.campaign, 
       CASE WHEN trafficSource.medium = "(none)" THEN "direct"
        ELSE trafficSource.medium END AS medium,
       CASE
        WHEN device.operatingSystem = "(not set)"
        THEN "others"
        WHEN device.deviceCategory = "(not set)"
        THEN "others"
        WHEN device.deviceCategory = "mobile"
        THEN device.operatingSystem
        ELSE device.deviceCategory
        END
       AS device,
       COALESCE(totals.pageViews, 0) + COALESCE(totals.screenViews, 0) AS views,
       totals.newVisits, totals.bounces, hit.social.socialInteractionNetwork

FROM  `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`,
      UNNEST(hits) AS hit
ORDER BY visitId, views DESC
-- LIMIT 100;