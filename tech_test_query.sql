select a.* from (select u.login_hash, u.server_hash, t.symbol ,u.currency,
date(t.close_time) as dt_report, 
sum(t.volume) over 
(partition by u.login_hash,u.server_hash,t.symbol order by date(t.close_time) rows between 6 preceding and current row) 
as sum_volume_prev_7d, 
sum(t.volume) over (partition by u.login_hash,u.server_hash,t.symbol order by date(t.close_time) rows between unbounded preceding and 1 preceding) 
as sum_volume_prev_all,
dense_rank() over 
(partition by u.login_hash,t.symbol order by volume desc,date(t.close_time) desc rows between 6 preceding and current row) 
as rank_volume_symbol_prev_7d,
dense_rank() over 
(partition by u.login_hash order by date(t.close_time) desc rows between 6 preceding and current row) 
as rank_count_prev_7d,
sum(CASE WHEN EXTRACT('Month' FROM t.close_time) = 8 then t.volume else 0 end) over 
(partition by u.login_hash,u.server_hash,t.symbol order by (t.close_time)) 
as sum_volume_2020_08,
first_value(date(t.open_time)) over 
(partition by u.login_hash,u.server_hash,t.symbol order by (t.close_time)) 
as date_first_trade,
row_number() over (partition by date(t.close_time),u.login_hash,u.server_hash,t.symbol order by date(t.close_time),u.login_hash,u.server_hash,t.symbol)
as row_number,
row_number() over (order by (select Null)) as id
from users u , trades t
where u.login_hash = t.login_hash 
and EXTRACT('DOW' FROM t.close_time) >= 1 and EXTRACT('DOW' FROM t.close_time) <=5
and EXTRACT('Month' FROM t.close_time) in (6,7,8,9) and EXTRACT('Year' FROM t.close_time) =2020
and u.enable =1
order by row_number desc) a
