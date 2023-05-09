--Total visit, pageview, transaction for Jan, Feb and March 2017 (order by month)
 SELECT FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d',date)) AS month,
        SUM(totals.visits) AS visits,
        SUM(totals.pageviews) AS pageviews,
        SUM(totals.transactions) AS transactions,
 FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
 WHERE _table_suffix BETWEEN '0101' AND '0331'
 GROUP BY 1
 ORDER BY 1;

 --Bounce rate per traffic source in July 2017 
WITH bounces_visits AS(
  SELECT trafficsource.source,
      SUM(totals.bounces) AS bounces,
      SUM(totals.visits) AS visits
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
  GROUP BY 1)
SELECT source, visits, bounces, 
      100*bounces/visits AS bounce_rate
FROM  bounces_visits
ORDER BY 3 DESC;

--Revenue by traffic source by week, by month in June 2017
WITH week AS(
  SELECT
    'Week' AS time_type,
    FORMAT_DATE('%Y%W',PARSE_DATE('%Y%m%d',date)) AS time, 
    trafficsource.source,
    SUM(product.productRevenue) AS revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
  UNNEST (hits) hits,
  UNNEST (hits.product) product
  WHERE product.productRevenue IS NOT NULL
  GROUP BY 3, 2),
month AS(
  SELECT
    'Month' AS time_type,
    FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d',date)) AS time,
    trafficsource.source,
    SUM(product.productRevenue) AS revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
  UNNEST (hits) hits,
  UNNEST (hits.product) product
  WHERE product.productRevenue IS NOT NULL
  GROUP BY 3, 2)

SELECT *
FROM Month
UNION ALL
SELECT *
FROM Week
ORDER BY source, time_type, time;

--Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017.
WITH purchaser_types AS
(SELECT
    fullvisitorId,
    date,
    totals.pageviews,
    CASE 
      WHEN totals.transactions >=1 AND product.productRevenue IS NOT NULL THEN 'purchaser'
      WHEN totals.transactions IS NULL AND product.productRevenue IS NULL THEN 'non-purchaser'
      END AS type
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  UNNEST (hits) hits,
  UNNEST (hits.product) product
  WHERE _table_suffix BETWEEN '0601' AND '0731'
),
purchasers AS(
SELECT 
   FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d',date)) AS month,
  SUM(pageviews)/COUNT(DISTINCT fullVisitorID) AS avg_pageviews_purchase
FROM purchaser_types 
WHERE type = 'purchaser'
GROUP BY 1
),
non_purchasers AS(
SELECT 
   FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d',date)) AS month,
  SUM(pageviews)/COUNT(DISTINCT fullVisitorID) AS avg_pageviews_non_purchase
FROM purchaser_types 
WHERE type = 'non-purchaser'
GROUP BY 1)

SELECT 
  month,avg_pageviews_purchase,avg_pageviews_non_purchase
FROM purchasers
LEFT JOIN non_purchasers
USING (month);


--Average number of transactions per user that made a purchase in July 2017

SELECT
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    sum(totals.transactions)/count(distinct fullvisitorid) as Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    ,unnest (hits) hits,
    unnest(product) product
WHERE  totals.transactions>=1
AND totals.totalTransactionRevenue is not null
AND product.productRevenue is not null
GROUP BY 1;

--Average amount of money spent per session. Only include purchaser data in July 2017
SELECT 
  FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d',date)) AS month,
  (SUM(product.productRevenue)/1000000)/COUNT(fullvisitorId) AS avg_spendpersession
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE totals.transactions IS NOT NULL AND product.productRevenue IS NOT NULL
GROUP BY 1;

--Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
WITH customer AS(
    SELECT 
      DISTINCT fullvisitorID,
      product.v2productname,
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    UNNEST (hits) hits,
    UNNEST (hits.product) product
    WHERE product.v2productname = "YouTube Men's Vintage Henley"
    AND product.productRevenue IS NOT NULL)

SELECT product.v2productname AS other_purchased_products,
      sum(product.productquantity) AS quantity
FROM customer
INNER JOIN `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
USING(fullvisitorID),
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE product.v2productname <> "YouTube Men's Vintage Henley"
AND product.productRevenue IS NOT NULL
GROUP BY product.v2productname
ORDER BY 2 DESC;

--Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. For example, 100% product view then 40% add_to_cart and 10% purchase.
WITH type_count AS(
  SELECT
      FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d',date)) AS month,
      SUM(CASE WHEN eCommerceAction.action_type = '2' THEN 1 ELSE 0 END) AS num_product_view,
      SUM(CASE WHEN eCommerceAction.action_type = '3' THEN 1 ELSE 0 END) AS num_addtocart,
      SUM(CASE WHEN eCommerceAction.action_type = '6' AND product.productRevenue IS NOT NULL THEN 1 ELSE 0 END) AS num_purchase,
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  UNNEST (hits) hits,
  UNNEST (hits.product) product
  WHERE _table_suffix BETWEEN '0101' AND '0331'
  GROUP BY 1
  ORDER BY 1)

SELECT month, num_product_view, num_addtocart,num_purchase,
      ROUND(100.0*num_addtocart/num_product_view,2) AS add_to_cart_rate,
      ROUND(100.0*num_purchase/num_product_view,2) AS purchase_rate
FROM type_count
GROUP BY 1,2,3,4
ORDER BY 1
