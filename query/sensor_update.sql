with
dataset1 as (
select
	date(intvStartTimeMs) as sourceDate,
	customerId,
	sensorVersion,
	if((toFloat32(substring(sensorVersion, 4, 2)) >= 1) or 
	   ((toFloat32(substring(sensorVersion, 4, 2)) = 0) and (toFloat32(substring(sensorVersion, 6, 2)) > 6)) or 
	   ((toFloat32(substring(sensorVersion, 4, 2)) = 0) and (toFloat32(substring(sensorVersion, 6, 2)) = 6) and (toFloat32(substring(sensorVersion, 8, 3)) > 5)),
	   'new', 
	   'old'
	  ) as sensorType,
	sum(intvEventCount) as eventNum
from app_sessionlet_pt1m_dist 
where 
	(
	(intvStartTimeMs between concat(toString(today()-3), ' 00:00:00') and concat(toString(today()-3), ' 03:00:00')) or 
	(intvStartTimeMs between concat(toString(today()-30), ' 00:00:00') and concat(toString(today()-30), ' 03:00:00'))
	)
	and platform = 'web'
	and inSession = 1
	and sensorVersion like '%js%'
group by 1,2,3,4
),

dataset2 as (
select 
	customerId, 
	sum(case when sensorType = 'new' then eventNum else null end) / sum(eventNum) as newSensorPct1
from dataset1
where sourceDate = today()-3
group by 1
),

dataset3 as (
select 
	customerId, 
	sum(case when sensorType = 'new' then eventNum else null end) / sum(eventNum) as newSensorPct2
from dataset1
where sourceDate = today()-30
group by 1
),

dataset5 as (
select
	a.customerId,
	a.newSensorPct1,
	b.newSensorPct2
from dataset2 a
left outer join dataset3 b
on a.customerId = b.customerId
),

dataset7 as (
select 
	customerId, 
	lower(replace(customerName, 'c3.', '')) as customerName
from app_emp_customerId_sampling_xuping 
where date(sample_date) = today()-3
),

dataset8 as (
select
	a.customerId,
 	b.customerName,
 	if((b.customerName like '%test') or (b.customerName like '%demo%') or (b.customerName like '%conviva%') or (b.customerName like '%internal%') or (b.customerName like '%dryrun%'), 'test-account', 'customer-account') as accountType,
 	a.newSensorPct1,
 	a.newSensorPct2
from dataset5 a 
left outer join dataset7 b 
on a.customerId = b.customerId
)

select 
	toString(customerId) as CustomerId, 
	toString(customerName) as CustomerName,
	toString(accountType) as AccountType,
	newSensorPct1 as newSensorPct_1d_ago,
 	newSensorPct2 as newSensorPct_1m_ago
from dataset8