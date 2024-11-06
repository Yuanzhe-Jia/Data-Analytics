-- json是clickhouse的实验特性, 如果建表语句包含json字段, 先开启实验特性
set allow_experimental_object_type = 1;

-- 下面的SQL最好与上面的SQL分开执行(分别复制 => 执行, 不要一次执行两条SQL, 因为有些IDE不支持)
CREATE TABLE IF NOT EXISTS default.temp_nlz_3hour_clean_data
Engine = MergeTree PARTITION BY toYYYYMMDD(eventTimeMs)
ORDER BY
(
customerId,
toStartOfMinute(eventTimeMs),
country,
city
) SETTINGS index_granularity = 8192,
storage_policy = 'big_space_storage' AS

-- 修改下面的逻辑即可
SELECT
customerId,
clientId,
userId,
sessionId,
sessionStartMs,
inSession,
eventId,
eventName,
eventTimeMs,
platform,
appName,
appBuild,
appVersion,
sensorVersion,
referrerHost,
host,
path,
query,
referrer,
title,
url,
userAgent,
deviceName,
deviceCategory,
deviceHardwareType,
deviceManufacturer,
deviceMarketingName,
deviceOperatingSystem,
deviceOperatingSystemVersion,
deviceOperatingSystemFamily,
deviceModel,
browserName,
browserVersion,
playerFrameworkName,
playerFrameworkVersion,
country,
state,
city,
isp,
asn,
postalCode,
networkRequestDurationMs
FROM
app_events_dist
WHERE
customerId = 1960183749
and eventTimeMs >= '2023-10-24 06:00:00' 
and eventTimeMs <= '2023-10-24 08:59:59'
and eventName not in ('conviva_periodic_heartbeat','conviva_network_request','conviva_page_ping')
