with hourly as
(select distinct extract(hour from convert_timezone('America/New_York', o.purchased_at)) as hour
from orders o
where [o.purchased_at:hest:day]='2019-12-31'
order by hour)

,daily as
(select
b.full_date as date
,day_name
,week_day_number
,year_day_number as calendar_day
,year_week_number as calendar_week
,month_number as calendar_month
,year_number as calendar_year
,c.day_of_year fiscal_day
,c.week_of_year fiscal_week
,c.month fiscal_month
,c.year fiscal_year
from calendar_b b
left join calendar_c c on b.full_date=c.date
where full_date between '2019-08-01' and [CURRENT_TIMESTAMP:hest:day]
order by full_date)

,calendar as
(select
date
,h.hour hour_num
,date||'-'|| case when len(h.hour)=1 then '0'||h.hour::text else h.hour::text end hour_key
,day_name
,week_day_number
,case when len(week_day_number)=1 then '0'||week_day_number::text else week_day_number::text end||'-'||hour_num day_hour
,calendar_day
,calendar_week
,calendar_month
,calendar_year
,fiscal_day
,fiscal_week
,fiscal_month
,fiscal_year
,fiscal_year||'-'||case when len(fiscal_month)=1 then '0'||fiscal_month::text else fiscal_month::text end fiscal_year_month
,calendar_year||'-'||case when len(calendar_month)=1 then '0'||calendar_month::text else calendar_month::text end calendar_year_month
,dense_rank() over (partition by fiscal_month||fiscal_year order by fiscal_week) as fiscal_week_month
,dense_rank() over (partition by fiscal_month||fiscal_year order by date) as fiscal_day_month
,dense_rank() over (partition by calendar_month||calendar_year order by calendar_week) as calendar_week_month
,dense_rank() over (partition by calendar_month||calendar_year order by date) as calendar_day_month
from hourly h
cross join daily d
order by date, hour
)

,sales2 as (
select [o.purchased_at:hest:day] as date
,[o.purchased_at:hest:hour] hour
,extract(hour from convert_timezone('America/New_York', o.purchased_at)) as hour_num
,coalesce(sum((o.price_cents + o.shipping_cents+coalesce(o.service_fee_cents,0))/100.0),0) as SALES
,case when [o.purchased_at:hest:day]<'2020-10-07' then 0 else coalesce(sum(case when coalesce(o.service_fee_cents,0)=0 then o.price_cents + o.shipping_cents end)/100.0,0) end as marketplace_SALES
,case when [o.purchased_at:hest:day]<'2020-10-07' then coalesce(sum((o.price_cents + o.shipping_cents)/100.0),0) else coalesce(sum(case when coalesce(o.service_fee_cents,0)>0 then o.price_cents + o.shipping_cents+coalesce(o.service_fee_cents,0) end)/100.0,0) end as shipsoon_SALES
,count(case when p.user_id <> o.user_id and o.price_cents > 0 then o.id end) as Orders
,case when [o.purchased_at:hest:day]<'2020-10-07' then 0 else count (case when p.user_id <> o.user_id and o.price_cents > 0 and coalesce(o.service_fee_cents,0)=0 then o.id end) end as marketplace_orders
,case when [o.purchased_at:hest:day]<'2020-10-07' then count(case when p.user_id <> o.user_id and o.price_cents > 0 then o.id end) else coalesce(count (case when p.user_id <> o.user_id and o.price_cents > 0 and coalesce(o.service_fee_cents,0)>0 then o.id end),0) end as shipsoon_orders
from orders o
join items p on p.id = o.product_id
join item_catalog as pt on pt.id = p.product_template_id
left join shipping_infos as sb on sb.id = o.to_buyer_shipping_info_id
where o.status not in ('canceled_by_buyer', 'fraudulent',
'pending', 'processing_payment', 'purchase_failed')
and o.platform_id = 2
and [o.purchased_at:hest:day] between '2019-08-01' and [CURRENT_TIMESTAMP:hest:day]
group by 1,2,3
order by 2
)

,sales1_items as (
select order_id,
sum(base_price) as sales,
count(*) as units
from sales_order_item_level
where product_type = 'changeable'
group by order_id
)

,sales1 as
(select
[o.created_at:hest:day] as date
, [o.created_at:hest:hour] as hour
,extract(hour from convert_timezone('America/New_York', o.created_at)) as hour_num
, sum(i.sales + o.base_shipping_amount) as SALES
, 0 as marketplace_sales
, sum(i.sales + o.base_shipping_amount) as shipsoon_SALES
, sum(i.units) as Orders
, 0 as marketplace_orders
,sum(i.units) as shipsoon_orders
from
sales1_item i
join sales_order_level o on
o.entity_id = i.order_id
where
[o.created_at:hest:day] between '2019-08-01' and [CURRENT_TIMESTAMP:hest:day]
and o.status in ('ready','picking','pick_issue','exception','complete')
group by 1,2,3
order by 2)

,sales as
(select
coalesce(s1.date,s2.date) as date
,coalesce(s1.hour,s2.hour) as hour
,case when len(coalesce(s1.hour_num,s2.hour_num))=1 then '0'||coalesce(s1.hour_num,s2.hour_num)::text else coalesce(s1.hour_num,s2.hour_num)::text end as hour_num
,coalesce(s1.date,s2.date)||'-'|| case when len(coalesce(s1.hour_num,s2.hour_num))=1 then '0'||coalesce(s1.hour_num,s2.hour_num)::text else coalesce(s1.hour_num,s2.hour_num)::text end as hour_key
,coalesce(s1.sales,0)+coalesce(s2.sales, 0) sales
,coalesce(s1.marketplace_sales,0)+coalesce(s2.marketplace_sales, 0) marketplace_sales
,coalesce(s1.shipsoon_sales,0)+coalesce(s2.shipsoon_sales, 0) shipsoon_sales
,coalesce(s1.orders,0)+coalesce(s2.orders, 0) orders
,coalesce(s1.marketplace_orders,0)+coalesce(s2.marketplace_orders, 0) marketplace_orders
,coalesce(s1.shipsoon_orders,0)+coalesce(s2.shipsoon_orders, 0) shipsoon_orders
,(coalesce(s1.sales,0)+coalesce(s2.sales, 0))::float/nullif(coalesce(s1.orders,0)+coalesce(s2.orders, 0),0) aov
,coalesce((coalesce(s1.marketplace_sales,0)+coalesce(s2.marketplace_sales,0))::float/nullif(coalesce(s1.marketplace_orders,0)+coalesce(s2.marketplace_orders, 0),0),0) marketplace_aov
,(coalesce(s1.shipsoon_sales,0)+coalesce(s2.shipsoon_sales, 0))::float/nullif(coalesce(s1.shipsoon_orders,0)+coalesce(s2.shipsoon_orders, 0),0) shipsoon_aov
from sales1 s1
full outer join sales2 s2 on s1.hour = s2.hour
order by 2)


,base as
(select
c.*
,case when len(c.fiscal_day_month)=1 then '0'||c.fiscal_day_month::text else c.fiscal_day_month::text end ||'-'||case when len(c.hour_num)=1 then '0'||c.hour_num::text else c.hour_num::text end as day_month_hour
,coalesce(s.sales,0) sales
,coalesce(s.orders,0) orders
,coalesce(s.aov,0) aov
,coalesce(s.marketplace_sales,0) marketplace_sales
,coalesce(s.marketplace_orders,0) marketplace_orders
,coalesce(s.marketplace_aov,0) marketplace_aov
,coalesce(s.shipsoon_sales,0) shipsoon_sales
,coalesce(s.shipsoon_orders,0) shipsoon_orders
,coalesce(s.shipsoon_aov,0) shipsoon_aov
from calendar c
left join sales s on c.hour_key=s.hour_key
order by c.date desc, c.hour_num)

,sales_now as
(select
*
from base
where hour_num=extract(hour from convert_timezone('America/New_York', current_timestamp))
and date=[current_timestamp:hest:day])


,monthly as
(select
fiscal_year_month
,fiscal_month
,fiscal_year
,max(day_month_hour) mtd_hour
,max(date) mtd
,sum(sales) sales
,sum(orders) orders
,sum(sales)::float/nullif(sum(orders),0) aov
,sum(marketplace_sales) marketplace_sales
,sum(marketplace_orders) marketplace_orders
,coalesce(sum(marketplace_sales)::float/nullif(sum(marketplace_orders),0),0) marketplace_aov
,sum(shipsoon_sales) shipsoon_sales
,sum(shipsoon_orders) shipsoon_orders
,sum(shipsoon_sales)::float/nullif(sum(shipsoon_orders),0) shipsoon_aov
from base
where hour_key<=(select hour_key from sales_now)
and day_month_hour<=(select day_month_hour from sales_now)
group by 1,2,3)

select
fiscal_year_month
,mtd_hour tm_mtd_hour
,sales as tm_sales
,marketplace_sales as tm_marketplace_sales
,shipsoon_sales as tm_shipsoon_sales
,orders as tm_orders
,marketplace_orders as tm_marketplace_orders
,shipsoon_orders as tm_shipsoon_orders
,aov as tm_aov
,marketplace_aov as tm_marketplace_aov
,shipsoon_aov as tm_shipsoon_aov
,lag(mtd,1) over (order by mtd) lm_mtd
,lag(sales,1) over (order by mtd) lm_sales
,lag(marketplace_sales,1) over (order by mtd) lm_marketplace_sales
,lag(shipsoon_sales,1) over (order by mtd) lm_shipsoon_sales
,lag(orders,1) over (order by mtd) lm_orders
,lag(marketplace_orders,1) over (order by mtd) lm_marketplace_orders
,lag(shipsoon_orders,1) over (order by mtd) lm_shipsoon_orders
,lag(aov,1) over (order by mtd) lm_aov
,lag(marketplace_aov,1) over (order by mtd) lm_marketplace_aov
,lag(shipsoon_aov,1) over (order by mtd) lm_shipsoon_aov
,lag(mtd,12) over (order by mtd) ly_mtd
,lag(sales,12) over (order by mtd) ly_sales
,lag(marketplace_sales,12) over (order by mtd) ly_marketplace_sales
,lag(shipsoon_sales,12) over (order by mtd) ly_shipsoon_sales
,lag(orders,12) over (order by mtd) ly_orders
,lag(marketplace_orders,12) over (order by mtd) ly_marketplace_orders
,lag(shipsoon_orders,12) over (order by mtd) ly_shipsoon_orders
,lag(aov,12) over (order by mtd) ly_aov
,lag(marketplace_aov,12) over (order by mtd) ly_marketplace_aov
,lag(shipsoon_aov,12) over (order by mtd) ly_shipsoon_aov
from monthly
order by mtd desc
limit 1
