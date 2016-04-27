members = LOAD '/demo/data/vk/bankdruzey_members.csv' USING PigStorage(',')
	AS (uid:int,
    	first_name:chararray,
        last_name:chararray,
        sex:int,
        bdate:chararray,
        country:int,
        city:int,
        deactivated:chararray);
DESCRIBE members;

members2 = FOREACH (FILTER members BY SIZE(bdate) > 5 AND deactivated IS NULL)
	GENERATE uid, sex, YearsBetween(CurrentTime(), ToDate(bdate,'d.M.y')) as age;
    
members_done = FOREACH (GROUP members2 BY (sex, age))
		   GENERATE FLATTEN(group) as (sex, age),
		   COUNT(members2);
           
DUMP members_done;
STORE members_done INTO '/demo/data/vk/members_done';