/*QUERIES FOR OPTIMIZATION*/
#1
#old query3: compare the percentage of cancelled flights for New york airport :
set profiling = 1;
select ORIGIN_AIRPORT_ID, count(*) as total_flights, COUNT(IF(CANCELLED = 1, 1, NULL)) as cancelled_flights  ,
concat((COUNT(IF(CANCELLED = 1, 1, NULL))/count(*))*100,'%')  as percentage
from january as j, candiv as c
where j.UNFLID = c.UNFLID and  j.ORIGIN_CITY_NAME = 'New York, NY'
group by ORIGIN_AIRPORT_ID
order by percentage desc;
show profiles;
#4.259


#new query
/* we do 1) create materalized table for origin_city in newyork 
		2) indexed the origin_airport_id
        3) indexed cancel on candiv*/
create table Newyork AS
	SELECT UNFLID,ORIGIN_AIRPORT_ID FROM january WHERE ORIGIN_CITY_NAME = 'New York, NY';
    
ALTER TABLE Newyork ADD INDEX airport (ORIGIN_AIRPORT_ID);

ALTER TABLE candiv ADD INDEX cancel (CANCELLED)	;

select ORIGIN_AIRPORT_ID, count(*) as total_flights, COUNT(IF(CANCELLED = 1, 1, NULL)) as cancelled_flights  ,
concat((COUNT(IF(CANCELLED = 1, 1, NULL))/count(*))*100,'%')  as percentage
	from Newyork as n, candiv as c
	where n.UNFLID = c.UNFLID 
	group by ORIGIN_AIRPORT_ID
	order by percentage desc;
show profiles;

#1.79

/*  query 5 The tail number of the plane that has more averaged flied distance in year 2015 
 and in which routh that plane flied more?*/
#2
#old
select j.TAIL_NUM , AVG(f.DISTANCE)
	from january as j, fsummary as f
	where j.UNFLID = f.UNFLID and 
	YEAR(STR_TO_DATE(j.FL_DATE, '%Y-%m-%d')) = 2015
	group by j.TAIL_NUM
	ORDER BY AVG(f.DISTANCE) DESC 
	LIMIT 10 ;
show profiles;
#6.151



#new
/* We do 1) create the January2015 table.
		 2) alter the columns type and index by Tail_NUM in jan2015*/
create table jan2015 AS
	SELECT UNFLID, TAIL_NUM FROM january WHERE YEARS = 2015;
    
ALTER TABLE jan2015
  MODIFY TAIL_NUM varchar(30);
  
ALTER TABLE jan2015 ADD INDEX tail_numb (TAIL_NUM);


select j.TAIL_NUM , AVG(f.DISTANCE)
	from jan2015 as j, fsummary as f
	where j.UNFLID = f.UNFLID 
	group by j.TAIL_NUM
	ORDER BY  AVG(f.DISTANCE) DESC 
	LIMIT 10 ;
SHOW PROFILES;         

#WOW 2.09 

#3
/*QUERY 6: What dat of the week we found more flights in 2010, 2015 and 2020?
#to solve this a ordinate way would be to create 3 view of our dataset, for january2020, january2015 and january2010 with the days and the number of flight in those days.*/
create view jan2020 as select day_of_week, count(day_of_week) as sumdays
						from january 
                        where YEAR(STR_TO_DATE(january.FL_DATE, '%Y-%m-%d')) = 2020
                        group by day_of_week;

create view jan2015 as select day_of_week, count(day_of_week) as sumdays
						from january 
                        where YEAR(STR_TO_DATE(january.FL_DATE, '%Y-%m-%d')) = 2015
                        group by day_of_week;

create view jan2010 as select day_of_week, count(day_of_week) as sumdays
						from january 
                        where YEAR(STR_TO_DATE(january.FL_DATE, '%Y-%m-%d')) = 2010
                        group by day_of_week;

#And now let's see the busiest day of the week for this three views
select a.day_of_week as day2020, b.day_of_week as day2015, c.day_of_week as day2010
 from jan2020 as a, jan2015 as b, jan2010 as c
 having max(a.sumdays)
	and max(b.sumdays)
	and max(c.sumdays);
 #where 3 is wednesday, 4 is thursday, 5 is friday! so during the years the busiest day of the month changed!!! People tend to travel more in the week than in the weekend!!
SHOW PROFILES;
#4.42 SECONDS
#to not forget let's drop the views for now
drop view jan2010;
drop view jan2015;
drop view jan2020;

#NEW
/* We do 1) We create tables with the days of the week, and index those table by year, so I simpler to retrieve the sum(day_of_the_week).
*/
create table ja2020 AS
	select day_of_week, count(day_of_week) as sumdays
						from january 
                        where YEARS = 2020
                        group by day_of_week;
create table ja2015 AS
	select day_of_week, count(day_of_week) as sumdays
						from january 
                        where YEARS = 2015
                        group by day_of_week;
create table ja2010 AS
	select day_of_week, count(day_of_week) as sumdays
						from january 
                        where YEARS = 2010
                        group by day_of_week;

ALTER TABLE ja2020 ADD INDEX d_o_week (DAY_OF_WEEK);
ALTER TABLE ja2015 ADD INDEX d_o_week (DAY_OF_WEEK);
ALTER TABLE ja2010 ADD INDEX d_o_week (DAY_OF_WEEK);

select a.day_of_week as day2020, b.day_of_week as day2015, c.day_of_week as day2010
 from ja2020 as a, ja2015 as b, ja2010 as c
 having max(a.sumdays)
	and max(b.sumdays)
	and max(c.sumdays);
show profiles;
#0.000415 seconds

#4

#old Q.9
/*Now, as we saw that Chigago was a mess of delays, let's list all the number 
flights that were cancelled on Thursdays(4), the busiest day according to previous queries and confonrt it to the max/min number of flights cancelled!*/

select *
from (select sum(cancelled) as s, day_of_week
		from january left outer join candiv
		on january.unflid = candiv.unflid
        where dest_city_name = 'Chicago, IL'
        or origin_city_name = 'Chicago, IL'
		group by day_of_week) as k
where k.day_of_week = 4 or k.s >= all( select sum(cancelled) as s
										from january left outer join candiv
										on january.unflid = candiv.unflid
										where dest_city_name = 'Chicago, IL'
										or origin_city_name = 'Chicago, IL'
                                        group by day_of_week)
						or  k.s <= all( select sum(cancelled) as s
										from january left outer join candiv
										on january.unflid = candiv.unflid
										where dest_city_name = 'Chicago, IL'
										or origin_city_name = 'Chicago, IL'
                                        group by day_of_week)
order by k.day_of_week asc;
show profiles;

# MORE THAN 400 SECONDS

#new
/* We do 1) We already indexed for ORIGIN_CITY_NAME january, now on january we index the DEST_CITY_NAME
		 2) We create in index for DAY_OF_WEEL for january*/
         
create table chic as        
         select unflid, day_of_week
			from january 
			where dest_city_name = 'Chicago, IL'
				or origin_city_name = 'Chicago, IL';
                
alter table chic add index  d_o_w (day_of_week);
set profiling = 1;          
select *
from (select sum(cancelled) as s, day_of_week
		from chic left outer join candiv
		on chic.unflid = candiv.unflid
		group by day_of_week) as k
where k.day_of_week = 4 or k.s >= all( select sum(cancelled) as s
										from chic left outer join candiv
											on chic.unflid = candiv.unflid
										group by day_of_week)
						or  k.s <= all(select sum(cancelled) as s
										from chic left outer join candiv
											on chic.unflid = candiv.unflid
										group by day_of_week)
order by k.day_of_week asc;
show profiles;

#9.28        
         
#5






/*Let's now see how much distance in jan2010, jan2015, jan2020 flights covered in the us confronted to how much distance covered from flights from or dest to NY and Chicago*/
#old
select Year, sum_distance, sumChicNY, sumChicNY/sum_distance
from 		(select YEAR(STR_TO_DATE(j.fl_date, '%Y-%m-%d')) as Year, sum(distance) as sum_distance
			  from fsummary as f, january as j
			  where j.unflid = f.unflid
			  group by YEAR(STR_TO_DATE(j.fl_date, '%Y-%m-%d')) ) as un, 
			(select YEAR(STR_TO_DATE(j.fl_date, '%Y-%m-%d')) as ChicNY, sum(distance) as sumChicNY
			  from fsummary as f, january as j
			  where j.unflid = f.unflid and (dest_city_name = 'Chicago, IL'
												or origin_city_name = 'Chicago, IL'
                                                or dest_city_name = 'New York, NY'
												or origin_city_name = 'New York, NY')
			  group by YEAR(STR_TO_DATE(j.fl_date, '%Y-%m-%d')) ) du
where un.Year = du.ChicNY;
show profiles;
# 15.951 seconds

#new
/* */
create table chicnew as       
         select unflid, years
			from january
			where dest_city_name = 'Chicago, IL'
				or origin_city_name = 'Chicago, IL'
                or dest_city_name = 'New York, NY'
				or origin_city_name = 'New York, NY';
                
alter table chicnew add index yearz (years);


select Year, sum_distance, sumChicNY, sumChicNY/sum_distance
from 		(select j.YEARS as YEAR, sum(distance) as sum_distance
			  from fsummary as f, january as j
			  where j.unflid = f.unflid
			  group by YEAR ) as un, 
			(select c.YEARS as ChicNY, sum(distance) as sumChicNY
			  from fsummary as f, chicnew as c
			  where c.unflid = f.unflid
			  group by ChicNY) du
where un.Year = du.ChicNY;
show profiles;
#13 seconds


