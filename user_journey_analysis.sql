with
dataset as (
  select
      clientId as clid,
      eventTimeMs as tstamp,
      title as current_name
  from 
      dt_data
  where 
      title is not null and title <> ""
),

dateset_1 as (
  select 
      *,
      lag(current_name) over (partition by clid order by tstamp) as former_name,
      if(current_name = "stctv_app.AppLaunchViewController", 1, 0) as is_target,
      lag(if(current_name = "stctv_app.AppLaunchViewController", 1, 0)) over (partition by clid order by tstamp) as former_target
  from 
      dataset
),

dateset_2 as (--remove continuous events
  select 
      *
  from 
      dateset_1
  where 
      current_name <> former_name
),

dateset_3 as (
  select
      *,
      if(lag(current_name, 1) over(partition by clid order by tstamp) = "stctv_app.AppLaunchViewController", 1, 0) as is_1_near,
      if(lag(current_name, 2) over(partition by clid order by tstamp) = "stctv_app.AppLaunchViewController", 1, 0) as is_2_near,
      if(lag(current_name, 3) over(partition by clid order by tstamp) = "stctv_app.AppLaunchViewController", 1, 0) as is_3_near
  from 
      dateset_2
),

dateset_4 as (
  select
      *,
      if((is_1_near = 1) or (is_2_near = 1) or (is_3_near = 1), 1, 0) as is_target_near
  from 
      dateset_3
),

dateset_5 as (
  select
      * except(former_target, is_target_near),
      case when is_target = 1 then "is_target" when is_target_near = 1 then "near_target" else null end as label
  from 
      dateset_4
  where 
      is_target = 1 or is_target_near = 1
)

select * from dateset_5 order by clid, tstamp
