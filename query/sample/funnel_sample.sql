WITH sub AS (
  SELECT
    visitId,
    visitStartTime,
    hit.transaction.transactionId,
    hit.transaction.transactionRevenue,
    p.v2ProductName,
    p.v2ProductCategory,
    geoNetwork.country
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`,
    UNNEST(hits) AS hit,
    UNNEST(hit.product) AS p
  WHERE
    hit.transaction.transactionRevenue IS NOT NULL
  LIMIT 100
),

hit_actions AS (
  SELECT
    visitId,
    hit.hitNumber AS action_step,
    hit.eCommerceAction.action_type AS action_type
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`,
    UNNEST(hits) AS hit
  WHERE
    hit.eCommerceAction.action_type IN ("1", "2", "3", "6")
    AND NOT (
      hit.eCommerceAction.action_type = "6"
      AND hit.transaction.transactionRevenue IS NULL
    )
)

SELECT
  sub.visitId,
  sub.visitStartTime,
  sub.v2ProductName,
  hit_actions.action_step,
  hit_actions.action_type
FROM
  sub
LEFT JOIN
  hit_actions
ON
  sub.visitId = hit_actions.visitId
ORDER BY
  sub.visitId;
