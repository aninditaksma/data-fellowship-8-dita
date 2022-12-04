SELECT 
date, 
ARRAY_AGG(STRUCT(geoNetwork_country, channelGrouping,totals_trans))
FROM 
(
  SELECT 
  date,
  geoNetwork_country,
  channelGrouping,
  SUM(totals_transactions) AS totals_trans,
  FROM `data-to-insights.ecommerce.rev_transactions`
  GROUP BY date, geoNetwork_country, channelGrouping
  ORDER BY geoNetwork_country
)
WHERE geoNetwork_country != '(not set)'
GROUP BY date
ORDER BY date
