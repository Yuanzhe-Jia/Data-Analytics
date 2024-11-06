with 
dataset_1 as (
select 
platform,
appName,
deviceName,
title,
clientId,
eventTimeMs,
eventName as event_1,
leadInFrame(eventName,1) over (partition by clientId order by eventTimeMs rows between unbounded preceding and unbounded following) as event_2,
leadInFrame(eventName,2) over (partition by clientId order by eventTimeMs rows between unbounded preceding and unbounded following) as event_3

from app_events_dist

where 
customerId = 1960183749
and toDate(eventTimeMs) = '2023-10-24'
and eventName not in ('conviva_network_request','conviva_periodic_heartbeat','conviva_page_ping')
and (eventName like 'conviva_%' or eventName like 'Open%' or eventName like 'Login%' or eventName like '%Play%' or eventName like 'Search%')
),

dataset_2 as (
select 
*
from dataset_1
where 
event_1 = 'Login Screen View' and event_2 <> 'Login Screen View' and event_3 <> 'Login Screen View' and event_3 <> ''
)

select 
platform,
appName,
deviceName,
title,
eventTimeMs,
concat(event_1, '_1') as event_1,
concat(event_2, '_2') as event_2
from dataset_2

union all

select 
platform,
appName,
deviceName,
title,
eventTimeMs,
concat(event_2, '_2') as event_2,
concat(event_3, '_3') as event_3
from dataset_2
