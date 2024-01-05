with
data_0 as (
  select
    clientId as clid,
    eventTimeMs as tstamp,
    title as current_name
  from intigral
  where title is not null and title <> "" and date(eventTimeMs) = "2023-12-28"
),

data_1 as (
  select 
    *,
    lag(current_name) over (partition by clid order by tstamp) as former_name,
    if(current_name = "stctv_app.AppLaunchViewController", 1, 0) as is_target,
    lag(if(current_name = "stctv_app.AppLaunchViewController", 1, 0)) over (partition by clid order by tstamp) as former_target
  from data_0
),

data_2 as (--remove continuous titles
  select 
    *
  from data_1
  where 
    current_name <> former_name
),

data_3 as (
  select
    *,
    if(lag(current_name, 1) over(partition by clid order by tstamp) = "stctv_app.AppLaunchViewController", 1, 0) as is_1_near,
    if(lag(current_name, 2) over(partition by clid order by tstamp) = "stctv_app.AppLaunchViewController", 1, 0) as is_2_near,
    if(lag(current_name, 3) over(partition by clid order by tstamp) = "stctv_app.AppLaunchViewController", 1, 0) as is_3_near
  from data_2
),

data_4 as (
  select
    *,
    if((is_1_near = 1) or (is_2_near = 1) or (is_3_near = 1), 1, 0) as is_target_near
  from data_3
),

data_5 as (
  select
    * except(former_name, former_target, is_target_near),
    case when is_target = 1 then "is_target" when is_target_near = 1 then "near_target" else null end as label
  from data_4
  where 
    is_target = 1 or is_target_near = 1
),

data_6 as (
  select
    *,
    lead(current_name, 1) over(partition by clid order by tstamp) as is_1_follow,
    lead(current_name, 2) over(partition by clid order by tstamp) as is_2_follow,
    lead(current_name, 3) over(partition by clid order by tstamp) as is_3_follow   
  from data_5
),

data_7 as (--devices in the initial period with login activities
  select 
    clid, 
    count(1) as cnt
  from data_6 
  --where is_target = 1 and is_1_follow = "stctv_app.NewLoginVC" and is_2_follow = "UIAlertController" and is_3_follow is not null and is_3_follow <> "stctv_app.AppLaunchViewController"
  where is_target = 1 and is_1_follow = "stctv_app.NewLoginVC" and is_2_follow = "stctv_app.ProfileSelectionViewController" and is_3_follow is not null and is_3_follow <> "stctv_app.AppLaunchViewController"
  group by 1
),

data_8 as (--devices in the follow-up period with login activities
  select
    clientId as clid
  from intigral
  where title is not null and title <> "" and title = "stctv_app.NewLoginVC" and date(eventTimeMs) <> "2023-12-28"
  group by 1
),

data_9 as (--churn analysis
  select 
    a.clid as clid_1, 
    b.clid as clid_2
  from data_7 a left outer join data_8 b 
  on a.clid = b.clid
)

select count(case when clid_2 is null then 1 else null end) / count(1) as churn_rate from data_9