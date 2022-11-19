
/*using the sampel set before running any of the real queries*/




##1) missing due dates
SELECT 
    '10k sample' AS Sample,
    ROUND(100 * (SUM(CASE
                WHEN `due date` IS NULL THEN 1
                ELSE 0
            END) / COUNT(*)),
            2) AS `% Missing Due Date`
FROM
    nyc311.sr_10k 
UNION ALL SELECT 
    '1k sample',
    ROUND(100 * (SUM(CASE
                WHEN `due date` IS NULL THEN 1
                ELSE 0
            END) / COUNT(*)),
            2) AS `% Missing Due Date`
FROM
    nyc311.sr_1k;


#2)created date after closed date
select '10k sample' as Sample,100 * round((sum(case when `created date` > `closed date` then 1 else 0 end) /count(`unique key`)),3) as '% Created Date After Closed Date' 
 from nyc311.sr_10k as t1
 union all
 select '1k sample' ,100 * round((sum(case when `created date` > `closed date` then 1 else 0 end) /count(`unique key`)),3) as '% Created Date After Closed Date' 
 from nyc311.sr_1k as t1;
 
 
 
 
 
 
 ## 3) due date before created date
 select '10k sample' as Sample,100 * round((sum(case when `due date` < `created date` then 1 else 0 end) /count(`unique key`)),3) as '% Due Date Before Created Date' 
 from nyc311.sr_10k as t1
 union all
 select '1k sample' ,100 * round((sum(case when `due date` < `created date` then 1 else 0 end) /count(`unique key`)),3)
 from nyc311.sr_1k as t1;
 
 


#4) Status not match closed date 
select @totalRows:= count(`unique key`) from nyc311.sr_10k;
select @totalRows2:= count(`unique key`) from nyc311.sr_1k;
 
 select '10k' as 'sample' ,sum(`% Status not match Closed Date `) as ` % Status not match Closed Date` from 
 (select 'Closed Null but Status Closed' as 'Match Fail Type' ,100 * count(`unique key`) / @totalRows as `% Status not match Closed Date ` from 
 nyc311.sr_10k where `closed date` is null and `status` = 'Closed'
 union all
 #select `unique key`, `closed date`, `status` from 
 select 'Closed Value but Status Not Closed' as 'type', 100 * count(`unique key`) / @totalRows from 
 nyc311.sr_10k where `closed date` is not null and `status` in ('Open','Assigned','Pending','In Progress')) as t2
 union all
 
 select '1k',sum(`% Status not match Closed Date `) as ` % Status not match Closed Date` from 
 (select 'Closed Null but Status Closed' as 'Match Fail Type' ,100 * count(`unique key`) / @totalRows2 as `% Status not match Closed Date ` from 
 nyc311.sr_1k where `closed date` is null and `status` = 'Closed'
 union all
 #select `unique key`, `closed date`, `status` from 
 select 'Closed Value but Status Not Closed' as 'type', 100 * count(`unique key`) / @totalRows2 from 
 nyc311.sr_1k where `closed date` is not null and `status` in ('Open','Assigned','Pending','In Progress')) as t2;
 
 

 ##5) zip Missingness

select @totalCounts2:= sum(`Count`) from nyc311.sr_incident_zip_summary;
select 100 * sum(Count) / @totalCounts2 as `% Missing Zip`
from nyc311.sr_incident_zip_summary a
where `incident zip` is  null or `incident zip`  in ("");


##6) zips that are not in the reference dataset 'zip_code_nyc_borough'
with totalCounts as (select sum(Count) as TotalCount from nyc311.sr_incident_zip_summary) 
SELECT 100 * sum(Count) / (select sum(TotalCount) from (table totalCounts) c) as `% Zip no Borough`
    FROM
        (`nyc311`.`sr_incident_zip_summary` `a`
        LEFT JOIN `nyc311`.`zip_code_nyc_borough` `b` ON ((`a`.`Incident Zip` = `b`.`Zip`)))
    WHERE
        (`b`.`Zip` IS NULL);

##or alternatively 
with reftable1 as (
select a.Count, c.Borough
from nyc311.sr_incident_zip_summary a 
	join nyc311.map_incident_zip_nyc_borough b on a.`incident zip` = b.`incident zip` 
		left join nyc311.zip_code_nyc_borough c on b.`zip` = c.`zip`)
select 100* sum(Count) / (select sum(Count) from (table reftable1) a2) as `% Zip no Borough` from (table reftable1) a1
where Borough is null
			
 
##7) Invaid Zip Codes 
 
 select sum(count) / @totalCounts3  * 100 as `%Invalid Zip Codes`
 from nyc311.sr_incident_zip_summary where not (`incident zip` REGEXP '^[0-9]{5}$'); #and not `incident zip` is null;
 ##it seems based on the map_incident_zip_nyc_borough table that the valid format for a zip is a 5 digit number
 

	
##complaint type FIELD quality

## 8)  Complaint Types not in  Reference Data
SELECT (SUM(Count) / (SELECT SUM(Count) FROM nyc311.sr_complaint_type_summary)) * 100 as 'No Reference Match %'
FROM nyc311.sr_complaint_type_summary
WHERE `Complaint Type`  not IN (SELECT `Type` FROM nyc311.ref_sr_type_nyc311_open_data_26 union select `problem_area` from nyc311.ref_problem_areas_nyc311_portal_57) ;


## 9) Borough Zip consistency. Percentage of boroughs in each table that do not refer to the same incident zip int the data sample
select '10k sample'as Sample,sum(case when a.Borough != b.Borough then 1 else 0 end) / count(`unique key`) * 100 as `%Unmatched Boroughs`
from nyc311.service_request_sample_10k a 
	join nyc311.zip_code_nyc_borough b on a.`Incident Zip` = b.`zip` 
    
union
select '1k sample'as Sample,sum(case when a.Borough != b.Borough then 1 else 0 end) / count(`unique key`) * 100 as `%Unmatched Boroughs`
from nyc311.service_request_sample_1k a 
	join nyc311.zip_code_nyc_borough b on a.`Incident Zip` = b.`zip`;


##10 ) park boroughs that are Inconsistent with refence Data
with reftable as (select `borough` from nyc311.zip_code_nyc_borough)
SELECT '10k' as Sample ,(Count(*) / (SELECT Count(*) FROM nyc311.service_request_sample_10k)) * 100 as `% Not Match park boroughs`
FROM nyc311.service_request_sample_10k
where `park borough` not in (table reftable)
union
SELECT '1K', (Count(*) / (SELECT Count(*) FROM nyc311.service_request_sample_1k)) * 100 as `% invalid park boroughs`
FROM nyc311.service_request_sample_1k
where `park borough` not in (table reftable);





##11)park borough incompleteness 
with reftable as (select `borough` from nyc311.zip_code_nyc_borough)
SELECT '10k' as sample, (Count(*) / (SELECT Count(*) FROM nyc311.service_request_sample_10k)) * 100 as `% Missing park boroughs`
FROM nyc311.service_request_sample_10k
where `park borough` is null
union
SELECT '1k' as sample, (Count(*) / (SELECT Count(*) FROM nyc311.service_request_sample_1k)) * 100 as `% Missing park boroughs`
FROM nyc311.service_request_sample_1k
where `park borough` is null;




## 12) testing the completeness of the start dates 
select 100 * count(*)/ 10000 as `% Complete Created Date ` from nyc311.sr_10k
where `created date` is not null ;





/*


 
 /*the subquery itself
 select 'Closed Null but Status Closed' as 'Match Fail Type' ,100 * count(`unique key`) / @totalRows as `% Status not match Closed Date ` from 
 nyc311.sr_10k where `closed date` is null and `status` = 'Closed'
 union all
 #select `unique key`, `closed date`, `status` from 
 select 'Closed Value but Status Not Closed' as 'type', 100 * count(`unique key`) / @totalRows from 
 nyc311.sr_10k where `closed date` is not null and `status` in ('Open','Assigned','Pending','In Progress');
 


##another way to write this
##association between service request types / problem areas and "the available values"
###how many requests do not fall into these categories. this falls under data consistency
select @totalCounts4:= sum(`Count`) from nyc311.sr_complaint_type_summary;
select sum(Count) as `Freq. Not Existing TYpe Category`,100 * sum(Count) / @totalCounts4 as `% Not Existing Type Category Requests` from  nyc311.sr_complaint_type_summary
where `complaint type` not in (select `type` from nyc311.ref_sr_type_nyc311_open_data_26);


##6)
select @totalCounts3  := sum(Count) from nyc311.sr_incident_zip_summary;
select sum(a.Count) * 100 / @totalCounts3 as `No Borough %`
from nyc311.sr_incident_zip_summary a 
	join nyc311.map_incident_zip_nyc_borough b on a.`incident zip` = b.`incident zip` 
		#left join nyc311.zip_code_nyc_borough c on b.`zip` = c.`zip`;
    where a.`Incident Zip` is not null and b.`Zip` is null;



SELECT 'Complaint Type' as  'Reference Match %', (SUM(Count) / (SELECT SUM(Count) FROM nyc311.sr_complaint_type_summary)) * 100 as value
FROM nyc311.sr_complaint_type_summary
WHERE `Complaint Type`   IN (SELECT `Type` FROM nyc311.ref_sr_type_nyc311_open_data_26) 
-- 11,024,708 / 27,736,190 = 39.7485
union all
##it seems there are some problem areas that have filtered through to complaint types as well. Will treat these as similar things
select  'Problem Area', (SUM(Count) / (SELECT SUM(Count) FROM nyc311.sr_complaint_type_summary)) * 100
from nyc311.sr_complaint_type_summary
where  `complaint type`   in (select `problem_area` from nyc311.ref_problem_areas_nyc311_portal_57);

