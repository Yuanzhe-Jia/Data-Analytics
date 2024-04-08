with 
dataset as (
  select 
    *,
    TIMESTAMP_MILLIS(EventTimeMs) as derived_tstamp,
    TIMESTAMP_MILLIS(cast(navigationStart as bigint)) as navigation_start,
    TIMESTAMP_MILLIS(cast(loadEventEnd as bigint)) as load_event_end,
    concat(page_urlhost, page_urlpath) as page_url2,
    row_number() over(partition by event_id order by EventTimeMs desc) as rn
 FROM event_table
 where in_session=1 and substr(v_tracker,1,3) = 'js-' 
),  

dedup_dataset as (
  select 
    *
  from dataset
  where rn = 1
),
 
page_change_mute_pv as (  
  select 
    *,
    if(page_url2 <> lag(page_url2) over(partition by session_id order by derived_tstamp), 1, 0) as is_page_change
  from dedup_dataset 
  where event_name <> "page_view"

  union all
  
  select 
    *,
    0 as is_page_change
  from dedup_dataset
  where event_name = "page_view"
  ),

events_with_page_id as (  
  select 
    *,
    sum(is_page_change) over(partition by session_id order by derived_tstamp) as page_id,
    TIMESTAMPDIFF(MILLISECOND, navigation_start, load_event_end) / 1000 as load_duration
  from page_change_mute_pv 
  ),

minutes_per_page as (  
  select 
    session_id, 
    page_id,
    TIMESTAMPDIFF(MILLISECOND, min(derived_tstamp), max(derived_tstamp)) / 1000 / 60 as page_duration
  from events_with_page_id 
  group by 1,2
  ),

previous_load as (  
  select
    *,
    lag(max_load_event_end) over(partition by session_id order by page_id) as previous_load_event_end
  from
  (select
    session_id,
    page_id,
    max(load_event_end) as max_load_event_end
  from events_with_page_id
  group by 1,2
  ) as a
  ),

is_larger_load as (
  select 
    a.session_id, 
    a.page_id, 
    a.event_name, 
    a.load_duration, 
    a.derived_tstamp,
    a.load_event_end, 
    b.previous_load_event_end, 
    if(a.load_event_end > b.previous_load_event_end, 1, 0) as is_larger_than_previous
  from events_with_page_id a
  join previous_load b
  on a.session_id = b.session_id and a.page_id = b.page_id
  where a.load_duration > 0
  ),

page_load_complete as (
  select 
    * 
  from is_larger_load 
  where page_id = 0

  union all

  select 
    * 
  from is_larger_load 
  where page_id > 0 and is_larger_than_previous = 1
  ),

page_load_time as (
  select 
    session_id,
    page_id,
    avg(load_duration) as load_time
  from page_load_complete
  group by 1,2
  ),

dryrun_sessionlets as (
  select
    split(sessionId,'-')[1] as clid,
    sessionId as session_id,
    pageId as page_id,
    url as page_url,
    if(intvisJustPageSwitched, 1, 0) as is_page_load,
    intvPageDurationMs / 1000 / 60 as page_duration,
    intvPageLoadSuccessCount as is_load_complete,
    intvPageLoadDurationMs / 1000 as load_duration
  from tlb_1min_table
  where 
    substr(sensorVersion,1,3) = 'js-'
    and inSession = true
  ),

ds_metrics as (
  select "page_loads" as metrics, "Web" as platform, count(distinct concat(session_id, page_id)) as ds_result from events_with_page_id --page loads
  union all 
  select "avg_minutes_per_page" as metrics, "Web" as platform, sum(page_duration) / count(distinct concat(session_id, page_id)) as ds_result from minutes_per_page  --avg minutes per page
  union all 
  select "page_load_complete" as metrics, "Web" as platform, count(distinct concat(session_id, page_id)) as ds_result from page_load_time --page load complete
  union all
  select "page_load_complete_rate" as metrics, "Web" as platform, b.page_load_complete / a.page_loads as ds_result from
  (select "1" as id, count(distinct concat(session_id, page_id)) as page_loads from events_with_page_id) a join (select "1" as id, count(distinct concat(session_id, page_id)) as page_load_complete from page_load_time) b on a.id = b.id  --page load complete rate
  union all 
  select "avg_page_load_time" as metrics, "Web" as platform, avg(load_time) as ds_result from page_load_time where load_time between 0 and 90 --avg page load time
  ),

tlb_metrics as (
  select "page_loads" as metrics, "Web" as platform, sum(is_page_load) as tlb_result from dryrun_sessionlets --page loads
  union all
  select "avg_minutes_per_page" as metrics, "Web" as platform, sum(page_duration) / sum(is_page_load) as tlb_result from dryrun_sessionlets --avg minutes per page
  union all
  select "page_load_complete" as metrics, "Web" as platform, sum(is_load_complete) as tlb_result from dryrun_sessionlets --page load complete
  union all
  select "page_load_complete_rate" as metrics, "Web" as platform, b.page_load_complete / a.page_loads as tlb_result from
  (select "1" as id, sum(is_page_load) as page_loads from dryrun_sessionlets) a join (select "1" as id, sum(is_load_complete) as page_load_complete from dryrun_sessionlets) b on a.id = b.id  --page load complete rate
  union all 
  select "avg_page_load_time" as metrics, "Web" as platform, sum(load_duration) / sum(is_load_complete) as tlb_result from dryrun_sessionlets --avg page load time
  )

select 
  a.metrics, 
  a.platform os_platform, 
  a.ds_result, 
  b.tlb_result, 
  round(if(a.metrics in ("page_load_complete_rate"), a.ds_result - b.tlb_result, (a.ds_result - b.tlb_result) / a.ds_result), 3) as gap 
from ds_metrics a 
join tlb_metrics b 
on a.metrics = b.metrics 
order by case when a.metrics = "page_loads" then 1 when a.metrics = "total_minutes" then 2 when a.metrics = "avg_minutes_per_page" then 3 when a.metrics = "page_load_complete" then 4 when a.metrics = "page_load_complete_rate" then 5 else 6 end
