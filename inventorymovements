	
	with received as
	(select
	event_family
	,event_name
	,o.srl_id
	,s.id itemid
	,message
	,s.user_id
	,warehouse
	,[timestamp:hpst:day] as event_date
	from items_log o
	left join items s on s.srl_id=o.srl_id
	join category_variants as v on s.v_id = v.id
	join product_master as p on v.product_id = p.id
	where warehouse is not null
	and event_family='location' and event_name='create'
	and (message like '%CON-A_2_3%' or message like '%BUY-A_2_3%' or message like '%DSHIP-A_2_3%' or message like '%WAREHOUSE-A_2_3%')
	and [timestamp:hpst:day]>='2019-01-01' and [s.received_at:hpst:day]>='2019-01-01'--*this is to prevent ops re-assigning old shoes to the consignment location
	and p.type = 'Shoe'
	and s.deleted_at is null
	order by o.srl_id, event_date desc)
	
	,listings as
	(select
	event_family
	,event_name
	,o.srl_id
	,s.id itemid
	,message
	,s.user_id
	,warehouse
	,[timestamp:hpst] as timestamp
	,rank() over(partition by o.srl_id order by timestamp asc) as event_rank
	from items_log o
	left join items s on s.srl_id=o.srl_id
	join category_variants as v on s.v_id = v.id
	join product_master as p on v.product_id = p.id
	where 1=1
	and o.srl_id is not null and o.srl_id<>'0000000000000'
	and p.type = 'Shoe'
	and s.deleted_at is null
	order by o.srl_id)	

	,activation as
	(select
	distinct itemid
	,srl_id
	,event_family
	,event_name
	,message
	,user_id
	,warehouse
	,[timestamp:day] as event_date
	,timestamp as activated_at
	from listings
	where event_rank=1)
	
	,sentout as
	(select
	event_family
	,event_name
	,o.srl_id
	,message
	,s.user_id
	,warehouse as out_warehouse
	,[timestamp:hpst:day] as event_date
	--,rank() over(partition by srl_id order by timestamp asc) as event_rank
	from items_log o
	left join items s on s.srl_id=o.srl_id
	join category_variants as v on s.v_id = v.id
	join product_master as p on v.product_id = p.id
	where warehouse is not null
	and o.srl_id is not null and o.srl_id<>'0000000000000'
	and event_family='stock-transfer' and event_name='assigned'
	and p.type = 'Shoe'
	and s.deleted_at is null
	order by o.srl_id, event_date desc)	

	,sentin as
	(select
	event_family
	,event_name
	,o.srl_id
	,message
	,s.user_id
	,warehouse as in_warehouse
	,[timestamp:hpst:day] as event_date
	--,rank() over(partition by srl_id order by timestamp asc) as event_rank
	from items_log o
	left join items s on s.srl_id=o.srl_id
	join category_variants as v on s.v_id = v.id
	join product_master as p on v.product_id = p.id
	where warehouse is not null
	and o.srl_id is not null and o.srl_id<>'0000000000000'
	and event_family='location' and event_name like '%warehouse-change%' and warehouse<>'IN TRANSIT'
	and p.type = 'Shoe'
	and s.deleted_at is null
	order by o.srl_id, event_date desc)
	
	,withdrawn as
	(select
	o.event_family
	,o.event_name
	,o.srl_id
	,o.message
	,s.user_id
	,o.warehouse
	,[o.timestamp:hpst:day] as event_date
	,a.activated_at
	--,rank() over(partition by srl_id order by timestamp asc) as event_rank
	from items_log o
	left join items s on s.srl_id=o.srl_id
	join category_variants as v on s.v_id = v.id
	join product_master as p on v.product_id = p.id
	left join activation a on a.srl_id=o.srl_id
	where o.warehouse is not null
	and o.srl_id is not null and o.srl_id<>'0000000000000'
	and o.event_family='location' and o.event_name='create' and o.message like '%WITHDRAWN-A_2_3%'
	and s.withdrawn_at>a.activated_at ---post activation withdrawn = true seller withdrawn
	and [o.timestamp:hpst:day]>='2019-01-01'
	and p.type = 'Shoe'
	and s.state in ('withdrawn')
	and s.deleted_at is null
	order by o.srl_id, event_date desc)
	
	,removed as
	(select
	o.event_family
	,o.event_name
	,o.srl_id
	,o.message
	,s.user_id
	,o.warehouse
	,[o.timestamp:hpst:day] as event_date
	,a.activated_at
	--,rank() over(partition by srl_id order by timestamp asc) as event_rank
	from items_log o
	left join items s on s.srl_id=o.srl_id
	join category_variants as v on s.v_id = v.id
	join product_master as p on v.product_id = p.id
	left join activation a on a.srl_id=o.srl_id
	where o.warehouse is not null
	and o.srl_id is not null and o.srl_id<>'0000000000000'
	and ((o.event_family='location' and o.event_name='create' and o.message like '%WITHDRAWN-A_2_3%' and (s.withdrawn_at<=a.activated_at or a.activated_at is null))
	or (o.event_family='inventory' and o.event_name='remove!')) 
	and [o.timestamp:hpst:day]>='2019-01-01'
	and p.type = 'Shoe'
	and s.state in ('withdrawn','removed')
	and s.deleted_at is null
	order by o.srl_id, event_date desc)

	,writeoff as
	(select
	event_family
	,event_name
	,o.srl_id
	,message
	,s.user_id
	,warehouse
	,[timestamp:hpst:day] as event_date
	--,rank() over(partition by srl_id order by timestamp asc) as event_rank
	from items_log o
	left join items s on s.srl_id=o.srl_id
	join category_variants as v on s.v_id = v.id
	join product_master as p on v.product_id = p.id
	where warehouse is not null
	and o.srl_id is not null and o.srl_id<>'0000000000000'
	and event_family='inventory' and event_name='write_off!'
	and [timestamp:hpst:day]>='2019-01-01'
	and p.type = 'Shoe'
	and s.deleted_at is null and s.state='sold'
	order by o.srl_id, event_date desc)	

	,rcv_sum as
	(select
	event_date as date
	,warehouse
	,count(distinct srl_id) as units
	from received
	where [event_date=DateRange]
	group by 1,2
	order by date desc)
	
	,sold_sum as
	(select
	[o.purchased_at:hpst:day] as date
	,w.name as warehouse
	--sum(o.price_cents+o.shipping_cents)/100.0 as gmv
	--count(case when p.user_id <> o.user_id and o.price_cents > 0 then p.id end) as orders
	-- ,(sum(o.price_cents/100.0))::numeric(12,2)/nullif(count(case when p.user_id <> o.user_id and o.price_cents > 0 then p.id end),0) as cl_aov
	,count(distinct p.id) as units
	from orders as o
	join product_master as p on
	o.product_id = p.id
	join product_templates pt on
	p.product_template_id = pt.id
	left join stock_items s on s.external_id=p.id
	left join warehouses w on w.id=s.warehouse_id
	where o.status not in ('canceled_by_buyer', 'fraudulent','pending', 'processing_payment', 'purchase_failed') 
	and [o.purchased_at=DateRange]
	and pt.product_type<=3
	group by 1,2
	order by 1 desc)
	
	,wtdn_sum as
	(select
	event_date as date
	,warehouse
	,count(distinct srl_id) as units
	from withdrawn
	where [event_date=DateRange]
	group by 1,2
	order by date desc)	

	,rmv_sum as
	(select
	event_date as date
	,warehouse
	,count(distinct srl_id) as units
	from removed
	where [event_date=DateRange]
	group by 1,2
	order by date desc)
	
	,off_sum as
	(select
	event_date as date
	,warehouse
	,count(distinct srl_id) as units
	from writeoff
	where [event_date=DateRange]
	group by 1,2
	order by date desc)
	
	,out_sum as
	(select
	event_date as date
	,out_warehouse
	,count(distinct srl_id) as units
	from sentout
	where [event_date=DateRange]
	group by 1,2
	order by date desc)
	
	,in_sum as
	(select
	event_date as date
	,in_warehouse
	,count(distinct srl_id) as units
	from sentsin
	where [event_date=DateRange]
	group by 1,2
	order by date desc)	

	,on_hand as
	(select [CURRENT_TIMESTAMP:hest:day] as date
	,w.name as warehouse
	,count(distinct s.srl_id) as units
	from [SQL Snippet]
	where
	p.type = 'Shoe' and s.deleted_at is null
	and s.state in ('on_sale','on_hold')
	group by 1,2)
	
	,summary1 as
	(select
	coalesce(rs.date,ss.date,ws.date,rms.date, os.date,ins.date,ofs.date,oh.date) as date
	,coalesce(rs.warehouse,ss.warehouse,ws.warehouse,rms.warehouse, os.out_warehouse,ins.in_warehouse,ofs.warehouse,oh.warehouse) as warehouse
	,rs.units as rcv_units
	,ins.units as sentin_units
	,ss.units as sold_units
	,ws.units as wtdn_units
	,rms.units as rmv_units
	,os.units as sentout_units
	,ofs.units as writeoff_units
	,oh.units as oh_units
	from rcv_sum rs
	full join sold_sum ss on rs.date=ss.date and rs.warehouse=ss.warehouse
	full join wtdn_sum ws on rs.date=ws.date and rs.warehouse=ws.warehouse
	full join rmv_sum rms on rs.date=rms.date and rs.warehouse=rms.warehouse
	full join out_sum os on rs.date=os.date and rs.warehouse=os.out_warehouse
	full join in_sum ins on rs.date=ins.date and rs.warehouse=ins.in_warehouse
	full join off_sum ofs on rs.date=ofs.date and rs.warehouse=ofs.warehouse
	full join on_hand oh on rs.date=oh.date and rs.warehouse=oh.warehouse
	order by date desc)
	
	,final as
	(select
	date
	--,case when lower(warehouse) like '%XYZ%' then 'XYZ'
	--when lower(warehouse) like '%transit%' then 'TRANSIT'
	--when lower(warehouse) like '%warehouse%' then 'DROP-SHIP' --or when lower(warehouse) like '%dropship%' or when lower(warehouse)='t') then 'DROP-SHIP'
	--when lower(warehouse) is null then 'NULL' else warehouse end as wh_name
	,coalesce(sum(rcv_units),0) rcv_units
	,coalesce(sum(sentin_units),0) sentin_units
	,coalesce(sum(sold_units),0) sold_units
	,coalesce(sum(wtdn_units),0) withdrawn_units
	,coalesce(sum(rmv_units),0) removed_units
	,coalesce(sum(sentout_units),0) sentout_units
	,coalesce(sum(writeoff_units),0) writeoff_units
	,coalesce(sum(oh_units),0) as on_hand_units
	,coalesce(sum(rcv_units),0)+coalesce(sum(sentin_units),0) total_inbound
	,coalesce(sum(sold_units),0)+coalesce(sum(wtdn_units),0)+coalesce(sum(rmv_units),0)+coalesce(sum(writeoff_units),0)+coalesce(sum(sentout_units),0) total_outbound
	--,coalesce(sum(activate_units),0) total_activation
	,coalesce(sum(oh_units),0)+coalesce(sum(sold_units),0)+coalesce(sum(wtdn_units),0)+coalesce(sum(rmv_units),0)+coalesce(sum(sentout_units),0)+coalesce(sum(writeoff_units),0)-coalesce(sum(rcv_units),0)-coalesce(sum(sentin_units),0) boh_var
	from summary1
	where [warehouse=warehouse_filter] - 
	group by 1
	order by 1 desc, total_inbound desc)
	
	,cal as
	(select
	f1.date as date
	,f1.total_inbound
	,f1.total_outbound
	,f1.rcv_units
	,f1.on_hand_units
	,f1.total_inbound-f1.total_outbound net
	,f1.boh_var
	,f1.sold_units
	,f1.removed_units
	,f1.sentin_units
	,f1.sentout_units
	,f1.withdrawn_units
	,f1.writeoff_units
	,sum(f2.boh_var) beg_balance
	--,sum(f2.boh_var)-(f1.inbound) eod_balance
	from final f1
	inner join final f2
	on f1.date<=f2.date
	group by 1,2,3,4,5,6,7,8,9,10,11,12,13
	order by 1 desc)
	
	select
	date
	,beg_balance
	,rcv_units receipts
	-- ,total_inbound as inbound
	,sold_units
	,withdrawn_units
	,sentout_units
	,sentin_units
	,removed_units
	,writeoff_units
	,total_outbound
	,net
	,beg_balance+total_inbound-total_outbound as end_balance
	--,sold_units
	--,withdrawn_units
	--,on_sale
	--,case when event_day=[getdate():hest:day] then on_sale else beg_balance+inbound-outbound end as end_balance
	from cal
	where date>='2020-12-18'
	order by 1 desc
	
