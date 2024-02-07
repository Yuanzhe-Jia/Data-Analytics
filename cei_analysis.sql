select
	fromUnixTimestamp(
        		  intDiv(toUnixTimestamp(intvStartTimeMs) - toUnixTimestamp('2024-01-20 00:00:00'), 3600 * 24) * 3600 * 24 
			  + toUnixTimestamp('2024-01-20 00:00:00')
		  	 ) AS cei_timestamp,
	
	(least(100, greatest(0, ((IF(sum(intvUserActiveTimeMs/1000/60) > 0,
	100.0 - ((IF(
    	uniqCombined(
	case when 
        	inSession = 1 and (
          	1 = 0
            	or (platform = 'mob' and intvAppCrashCount >= 1) 
            	or (platform = 'web' and intvIsJustPageSwitched = 1 and intvPageLoadDurationMs > 1000 * 5) 
            	or (platform = 'mob' and intvIsJustPageSwitched = 1 and intvPageLoadDurationMs > 1000 * 1)
		or (platform = 'mob' and intvAppStartupCount > 0 and intvAppStartupDurationMs > 1000 * 1)
            	
        )
	then
		concat(clientId, toString(toStartOfMinute(intvStartTimeMs)))
      	else
      		null
      	end
    	) is null,
      	0,
    	uniqCombined(
      	case when
        	inSession = 1 and (
          	1 = 0
            	or (platform = 'mob' and intvAppCrashCount >= 1) 
            	or (platform = 'web' and intvIsJustPageSwitched = 1 and intvPageLoadDurationMs > 1000 * 5) 
            	or (platform = 'mob' and intvIsJustPageSwitched = 1 and intvPageLoadDurationMs > 1000 * 1)
		or (platform = 'mob' and intvAppStartupCount > 0 and intvAppStartupDurationMs > 1000 * 1)
        )
      	then
        	concat(clientId, toString(toStartOfMinute(intvStartTimeMs)))
      	else
        	null
      	end
    	)
    	)) / sum(intvUserActiveTimeMs/1000/60) * 100.0),
    	100)))))) AS cei
from
	app_sessionlet_pt1m_dist
where 
	customerId = 1960183749 
	and ((intvStartTimeMs >= '2024-01-20 00:00:00' and intvStartTimeMs < '2024-01-24 00:00:00'))	
group by 1 with totals
