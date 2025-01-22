set allow_experimental_funnel_functions = 1;

with 
dataset_1 as (
select 
*
from app_events_dist
where 
customerId = 1960183749
and toDate(eventTimeMs) = '2023-10-28'
and eventName not in ('conviva_network_request','conviva_periodic_heartbeat','conviva_page_ping')
and (eventName like 'conviva_%' or eventName like 'Open%' or eventName like 'Login%' or eventName like '%Play%' or eventName like 'Search%')
),

dataset_2 as (
select 
appName,
deviceName,
title,
sessionId,
sequenceNextNode('forward', 'first_match')(
	toUInt64(toDecimal64(eventTimeMs, 3) * 1000),
	eventName,
	eventName = 'Login Screen View',
	eventName = 'Login Screen View'
	) as event
from dataset_1
group by 1,2,3,4
),

dataset_3 as (
select 
appName,
deviceName,
title,
sessionId,
sequenceNextNode('forward', 'first_match')(
	toUInt64(toDecimal64(eventTimeMs, 3) * 1000),
	eventName,
	eventName = 'Login Screen View',
	eventName = 'Login Screen View',
	eventName = 'conviva_screen_view'
	) as event
from dataset_1
group by 1,2,3,4
)

select 
appName, deviceName, title, 'Login Success View_1' as event_1, concat(event, '_2') as event_2 
from dataset_2 
where event is not null

union all 

select 
appName, deviceName, title, 'conviva_screen_view_2' as event_1, concat(event, '_3') as event_2 
from dataset_3 
where event is not null
