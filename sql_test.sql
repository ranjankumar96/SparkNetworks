3)The Product Owner asked you to provide the queries for some scenarios as they are wondering about possibilities of quality issues in the data,
  please add a file sql_test.sql in your project with the queries that solve the below questions:
	
	
1.•	How many total messages are being sent every day?

Select dateadd(minute, datediff(minute, 0, date)/1440 * 1440,0) as dateday,
count(*) as countmsgday
from user
group by dateadd(minute, datediff(minute, 0, date)/1440 * 1440,0);

2.•	Are there any users that did not receive any message?

Select u.*
From users u join
     messages m
     on u.userid = m.msgid
	 where u.userid in (m.senderid, m.receiverid)  
	and u.userid != $Userid 
	order by m.createdat desc
				 
3.How many active subscriptions do we have today?

Select count(distinct t.userid) from subscriptions
where status = 'active'
and cast(createdat as date1) = cast(getdate() as date1)

4. Are there users sending messages without an active subscription? 

Select u.*
From users u join subscription s 
on u.userid = s.userid
where s.status = 'rejected'


5. Did you identified any inaccurate/noisy record that somehow could prejudice the data analyses? How to monitor it 

Data records are correctly maintained, apart from:
a> we can ensure 'receiverId' is successfully transmitted with 'message' when status is 'active'  
b> using Snowflake DB & Dashboard Analytics, we can overcome the daily disturbancies like  'messages are being sent every day' , 'messages without an active subscription'

With cte_daily_run as(
select table_catalog,table_schema,table_name created, last_altered 
from information_schema.tables
where last_altered > dateadd(day, -1, current_timestamp)
      and table_type = 'base table'
  and table_schema ilike '%<schema_name>'
order by last_altered desc, table_schema asc)
select * from cte_daily_run;

c> App experience can provide me with further changes that I can apply to existing data.
