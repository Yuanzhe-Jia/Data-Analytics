with 
dataset_0 as
(
select 
	clientId, 
	sessionIdNew, 
	eventTimeMs, 
	eventName
from app_events_dist
where 
	customerId = 1960180521 --dryrun
	and url = 'https://pulse.conviva.com/app/experience-insights/dashboard/prompt' --Prompt AI
),

dataset_1 as 
(
select 
	*, 
	MAX(eventTimeMs) over(partition by clientId, sessionIdNew) as max_time,
	MIN(eventTimeMs) over(partition by clientId, sessionIdNew) as min_time
from dataset_0 
),

dataset_2 as
(
select distinct
	clientId, 
	sessionIdNew, 
	max_time, 
	date_diff('second', min_time, max_time) as time_diff 
from dataset_1 
),

dataset_3 as
(
select 
	toStartOfMinute(max_time) as minutes,
	sum(time_diff) / count(distinct clientId) as avg_engagement_per_user 
from dataset_2
group by 1
),

dataset_4 as
(
select 
	toStartOfMinute(eventTimeMs) as minutes
from app_events_dist
where 
  customerId = 1960180521 --dryrun
group by 1
)

select 
	a.minutes, 
	b.avg_engagement_per_user
from dataset_4 a
left join dataset_3 b 
on a.minutes = b.minutes
order by 1
