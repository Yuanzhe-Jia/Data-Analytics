with
dataset as (
select
    sessionId,
    eventTimeMs,
    eventName,
    leadInFrame(eventTimeMs, 1) over (partition by sessionId order by eventTimeMs rows between unbounded preceding and unbounded following) as timeBehind,
    leadInFrame(eventName, 1) over (partition by sessionId order by eventTimeMs rows between unbounded preceding and unbounded following) as eventBehind
from 
 	`default`.app_events_dist
where
 	customerId = 1960183749 --nlz
 	--customerId = 1960183601 --zee
 	--customerId = 1960180407 --rtve
 	--customerId = 1960180521 --dryrun
 	and platform = 'web'
	and date(eventTimeMs) between '2023-12-28' and '2023-12-31'	
	and eventName like '%conviva_%'
	and eventName <> 'conviva_application_error'
),

dataset_1 as (
select 
	sessionId, eventTimeMs, eventName, timeBehind,
	if(eventBehind = '', 'null', eventBehind) as eventBehind,
	dateDiff('second', eventTimeMs, timeBehind) as timeDiff
from dataset
where eventName = 'conviva_page_view'
),

dataset_2 as (
select 
	eventBehind,
	count(1) as cnt,
	round(count(1) / sum(count(1)) over(partition by 1), 3) as pct
from dataset_1
group by 1
),

dataset_3 as (
select 
	case when timeDiff = 0 then '0'
		 when timeDiff > 0 and timeDiff <= 1 then '0-1'
		 when timeDiff > 1 and timeDiff <= 2 then '1-2'
		 when timeDiff > 2 and timeDiff <= 3 then '2-3'
		 when timeDiff > 3 and timeDiff <= 5 then '3-5'
		 when timeDiff > 5 and timeDiff <= 10 then '5-10'
		 else '10+' end as tag,
	count(1) as cnt,
	round(count(1) / sum(count(1)) over(partition by 1), 3) as pct
from dataset_1
where timeDiff >= 0
group by 1
)

--select * from dataset_2 order by 2 desc

select * from dataset_3 order by 1