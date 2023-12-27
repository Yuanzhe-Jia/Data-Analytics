with
dataset as (
  select
    	deviceName,
    	browserVersion,
    	state,
    	eventName,
    	eventTimeMs
  from `default`.app_events_dist
  where
    	customerId = 1960180550
    	and date(eventTimeMs) between '2023-12-21' and '2023-12-23'
    	and eventName = 'conviva_application_error'
),

crash_t as (
  select * from dataset where date(eventTimeMs) = '2023-12-23'
),

crash_tn1 as (
  select * from dataset where date(eventTimeMs) = '2023-12-22'
),

crash_tn2 as (
  select * from dataset where date(eventTimeMs) = '2023-12-21'
),

dist_t as (
  select 
  	*,
  	sum(cnt) over(partition by dim) as total_cnt,
  	cnt / sum(cnt) over(partition by dim) as pct
  from
  (
    select 'deviceName' as dim, deviceName as tag, count(1) as cnt from crash_t group by 1,2 UNION ALL
    select 'browserVersion' as dim, browserVersion as tag, count(1) as cnt from crash_t group by 1,2
  ) a
),

dist_tn1 as (
  select 
  	*,
  	sum(cnt) over(partition by dim) as total_cnt,
  	cnt / sum(cnt) over(partition by dim) as pct
  from
  (
    select 'deviceName' as dim, deviceName as tag, count(1) as cnt from crash_tn1 group by 1,2 UNION ALL
    select 'browserVersion' as dim, browserVersion as tag, count(1) as cnt from crash_tn1 group by 1,2
  ) a
),

dist_tn2 as (
  select 
  	*,
  	sum(cnt) over(partition by dim) as total_cnt,
  	cnt / sum(cnt) over(partition by dim) as pct
  from
  (
    select 'deviceName' as dim, deviceName as tag, count(1) as cnt from crash_tn2 group by 1,2 UNION ALL
    select 'browserVersion' as dim, browserVersion as tag, count(1) as cnt from crash_tn2 group by 1,2
  ) a
),

join_tab1 as (
  select 
  	a.dim, a.tag, a.cnt as cnt_t, b.cnt as cnt_tn1, a.pct as pct_t, b.pct as pct_tn1
  from dist_t a join dist_tn1 b 
  on a.dim = b.dim and a.tag = b.tag
),

join_tab2 as (
  select 
  	a.dim, a.tag, a.cnt_t, a.cnt_tn1, b.cnt as cnt_tn2, a.pct_t, a.pct_tn1, b.pct as pct_tn2
  from join_tab1 a join dist_tn2 b 
  on a.dim = b.dim and a.tag = b.tag
),

psi_tab as (
  select
    	*,
    	count(1) over (partition by dim) as dim_cnt,
    	(pct_t - pct_tn1) * ln(pct_t / pct_tn1) as psi_t_tn1,
    	(pct_tn1 - pct_tn2) * ln(pct_tn1 / pct_tn2) as psi_tn1_tn2
  from join_tab2
),

rpsi_tab as (
  select
    	*,
    	psi_t_tn1 / psi_tn1_tn2 / dim_cnt as rpsi
  from psi_tab
)

select * from rpsi_tab order by dim, rpsi desc
