

--Q1
SELECT CAST(LEFT(date,6) AS NUMERIC) month
        -->format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
      ,sum(totals.visits) visit1
      ,sum(totals.pageviews) pageview1
      ,sum(totals.transactions) transaction1
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
where _table_suffix between '0101' and '0331'
group by month
order by month ;
--correct

--Q2
SELECT trafficsource.source
    ,100*(count(totals.bounces)/count(totals.visits)) rate1
    ,count(totals.visits) vs1
    ,count(totals.bounces) bc1
 FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
 where _table_suffix between '0701' and '0731'
 group by trafficSource.source
 order by vs1 desc;
--correct

--Q3
WITH 
week1 AS (
    select 'week' as time_type,
            format_date("%Y%W", parse_date('%Y%m%d', date)) as week,
            trafficSource.source as sou,
            sum ( product.productrevenue)/1000000 as rev
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
     unnest (hits) as hits,
     UNNEST(hits.product) AS product
    group by week,sou
), 
month1 AS (
    select 'month' as time_type,
            format_date("%Y%m", parse_date('%Y%m%d', date)) as month,
            trafficSource.source as sou,
            sum ( product.productrevenue)/1000000 as rev
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
     unnest (hits) as hits,
     UNNEST(hits.product) AS product
    group by month ,sou
)
SELECT * from week1
union all
SELECT * from month1
order by rev desc;
--correct
--chỉnh lại 1 chút trình bày cho dễ nhìn hơn

-- Q4
with 
pur as (
    SELECT  format_date("%Y%m", parse_date('%Y%m%d', date)) as month
          ,(sum(totals.pageviews)/count(distinct fullVisitorId)) pvpur  
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` ,
    unnest (hits) as hits,
    unnest (hits.product) as product2
    where _table_suffix between '0601' and '0731'
       and totals.transactions >= 1
       and productRevenue is not null
    group by month
),
nonpur as ( 
    SELECT format_date("%Y%m", parse_date('%Y%m%d', date)) as month
          ,sum(totals.pageviews) / count (distinct fullVisitorId) pvnonpur
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` ,
    unnest (hits) as hits,
    unnest (hits.product) as product2
    where _table_suffix between '0601' and '0731'
       and totals.transactions is null
       and productRevenue is null
    group by month)

select pvpur, pvnonpur
from pur  --> full join
full join nonpur using (month)



--Q5 
WITH purchase_users AS (
    SELECT
        fullVisitorId,
         sum(totals.transactions)/count(distinct fullVisitorId) as Avg_total_transactions_per_user
    FROM
        `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
        UNNEST(hits) AS hits,
        UNNEST(hits.product) AS product
    where  _table_suffix between '0701' and '0731'
        and  totals.transactions >= 1
        AND product.productRevenue IS NOT NULL
    GROUP BY
        fullVisitorId
)

select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    sum(totals.transactions)/count(distinct fullvisitorid) as Avg_total_transactions_per_user
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    ,unnest (hits) hits,
    unnest(product) product
where  totals.transactions>=1
and product.productRevenue is not null
group by month;



--Q6
SELECT format_date("%Y%m", parse_date('%Y%m%d', date)) as month
        ,round((sum(productRevenue) / count (fullvisitorid)/1000000),2) avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` ,
unnest (hits) as hits,
unnest (hits.product) as product2
where totals.transactions IS NOT NULL
  and productRevenue is not null
group by month
--correct

--Q7
WITH product as (
    SELECT
        fullVisitorId,
        product.v2ProductName,
        product.productRevenue,
        product.productQuantity 
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
        UNNEST(hits) as hits,
        UNNEST(hits.product) as product
    Where 
        
         product.productRevenue IS NOT NULL
)

SELECT
    product.v2ProductName as other_purchased_products,
    SUM(product.productQuantity) as quantity
FROM product
WHERE 
    product.fullVisitorId IN (
        SELECT fullVisitorId
        FROM product
        WHERE product.v2ProductName like "YouTube Men's Vintage Henley"

    )
    AND product.v2ProductName NOT like "YouTube Men's Vintage Henley"
GROUP BY other_purchased_products
ORDER BY quantity desc

--subquery:
select
    product.v2productname as other_purchased_product,
    sum(product.productQuantity) as quantity
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    unnest(hits) as hits,
    unnest(hits.product) as product
where fullvisitorid in (select distinct fullvisitorid
                        from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
                        unnest(hits) as hits,
                        unnest(hits.product) as product
                        where product.v2productname = "YouTube Men's Vintage Henley"
                        and product.productRevenue is not null)
and product.v2productname != "YouTube Men's Vintage Henley"
and product.productRevenue is not null
group by other_purchased_product
order by quantity desc;

--CTE:

with buyer_list as(
    SELECT
        distinct fullVisitorId
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    , UNNEST(hits) AS hits
    , UNNEST(hits.product) as product
    WHERE product.v2ProductName = "YouTube Men's Vintage Henley"
    AND totals.transactions>=1
    AND product.productRevenue is not null
)

SELECT
  product.v2ProductName AS other_purchased_products,
  SUM(product.productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
JOIN buyer_list using(fullVisitorId)
WHERE product.v2ProductName != "YouTube Men's Vintage Henley"
 and product.productRevenue is not null
GROUP BY other_purchased_products
ORDER BY quantity DESC;



--Q8

bài này mình có thể dùng count(case when) hoặc sum(case when)

with product_data as(
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    count(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,
    count(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,
    count(CASE WHEN eCommerceAction.action_type = '6' and product.productRevenue is not null THEN product.v2ProductName END) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
,UNNEST(hits) as hits
,UNNEST (hits.product) as product
where _table_suffix between '20170101' and '20170331'
and eCommerceAction.action_type in ('2','3','6')
group by month
order by month
)

select
    *,
    round(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate,
    round(num_purchase/num_product_view * 100, 2) as purchase_rate
from product_data;



                                                        ---good---



