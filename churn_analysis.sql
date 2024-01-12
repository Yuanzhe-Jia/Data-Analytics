with 
users_with_login as (
select 
  clientId, 
  count(case when eventName = 'Login Screen View' then 1 else null end) as login_attempts,
  count(case when eventName = 'Login Success' then 1 else null end) as login_success
  --clientId, sessionId, eventTimeMs, eventName
from `default`.app_events_dist 
where 
  toDate(eventTimeMs) = '2023-11-27' 
  and customerId = 1960183749
  --and eventName like '%Login%'
  and eventName in ('Login Screen View', 'Login Success')
group by 1
),

login_users_no_success as (
select 
*
from users_with_login
where login_attempts >= 1 and login_success = 0
),

later_login_users as (
select 
clientId, 
count(case when eventName = 'Login Screen View' then 1 else null end) as login_attempts
from `default`.app_events_dist 
where 
toDate(eventTimeMs) between '2023-11-28' and '2023-12-04' 
and customerId = 1960183749
group by 1 having login_attempts >= 1
),

join_table as (
select 
a.clientId as user_1, b.clientId as user_2
from login_users_no_success a
left outer join later_login_users b
on a.clientId = b.clientId
)

select 
count(user_1) as users,
count(case when user_2 = '' then 1 else null end) as churn_users,
count(case when user_2 = '' then 1 else null end) / count(user_1) as churn_rate
from join_table
