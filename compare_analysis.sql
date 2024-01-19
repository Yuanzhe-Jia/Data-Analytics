with
data_1 as (
select
	customerId
from 
	`default`.app_sessionlet_pt1m_dist
where
	customerId in (1960183749, 1960183601, 1960180407, 1960181213, 1960185375, 1960180521)
 	--customerId = 1960183749 --nlz
 	--customerId = 1960183601 --zee
 	--customerId = 1960180407 --rtve
 	--customerId = 1960181213 --bt	
 	--customerId = 1960185375 --fancode
 	--customerId = 1960181009 --intigral
 	--customerId = 1960180521 --dryrun	
	and date(intvEndTimeMs) = '2024-01-15'
),

data_2 as (
select
	customerId
from 
	`default`.app_sessionlet_pt30m_dist
where
	customerId in (1960183749, 1960183601, 1960180407, 1960181213, 1960185375, 1960180521) 	
	--customerId = 1960183749 --nlz
 	--customerId = 1960183601 --zee
 	--customerId = 1960180407 --rtve
 	--customerId = 1960181213 --bt	
 	--customerId = 1960185375 --fancode
 	--customerId = 1960181009 --intigral
 	--customerId = 1960180521 --dryrun	
	and date(intvEndTimeMs) = '2024-01-15'
)

select 
	case when a.customerId = 1960183749 then 'nlz'
  	     when a.customerId = 1960183601 then 'zee'
             when a.customerId = 1960180407 then 'rtve'
	     when a.customerId = 1960181213 then 'bt'
	     when a.customerId = 1960185375 then 'fancode'
	     else 'dryrun' end as customer,
	a.1min_sessionlet, 
	b.30min_sessionlet, 
	round(a.1min_sessionlet/b.30min_sessionlet, 3) as pct
from 
(select customerId, count(1) as 1min_sessionlet from data_1 group by 1) a
join
(select customerId, count(1) as 30min_sessionlet from data_2 group by 1) b
on a.customerId = b.customerId
order by pct desc
