use schema AJE98_MBIS623

--1) dimweek table extension
insert into dim_yearweek
SELECT DISTINCT yearofweek("Created_Date") * 100 + weekiso("Created_Date") as yearweek 
FROM nyc311.service_request_all
where yearweek >=  202206 -- filtering table to include only the new values
order by yearweek asc;

--creating a new complaint type dimension to include new type varieties
create or replace table dim_request_type (type_id number(8, 0) primary key, type_name varchar(60));
--initial build
insert into dim_request_type 
select "ID" as type_id, "Type" as type_name 
from nyc311."REF_SR_TYPE_NYC311_OPEN_DATA_26";

create or replace table ref_complaint_types as
select distinct case -- case statement consolidates some of the complaint types found in the initial datset
        when "Complaint_Type" like 'Misc%' then 'Miscellaneous'
        when "Complaint_Type" like '%Compliance%' then 'Compliance'
        when "Complaint_Type" like '%COVID%' then 'COVID'
        when lower("Complaint_Type") like '%Select%' then 'System Error'
        when "Complaint_Type" like '%../../%' then 'System Error'
        when "Complaint_Type" like '%....%' then 'System Error'
        when lower("Complaint_Type") like '%heat%' then 'Heating'
        when "Complaint_Type" like 'Paint' then 'Paint'
        when  "COUNT" <=2 then 'Other/Error'
        else "Complaint_Type"
        end as "Complaint_Type"
        
from "NYC311"."NYC311"."SR_COMPLAINT_TYPE_AGENCY_SUMMARY";


--populating new complaint_type values
insert into dim_request_type
with max_id as (select to_number(max("ID")) as start_num 
                from nyc311.REF_SR_TYPE_NYC311_OPEN_DATA_26), -- storing an id value to start incrementing
     reftable as (select "Complaint_Type" --creating a reference table for complaint_Type
                  from ref_complaint_types
                  where "Complaint_Type" not in 
                  (select distinct "Type" from nyc311.REF_SR_TYPE_NYC311_OPEN_DATA_26)) -- 
                  
select to_number(row_number() over (order by "Complaint_Type") + (select start_num from max_id),8, 0) as "ID","Complaint_Type" -- adding an incrementer starting with the maximum existing complaint ID
from reftable;


--QUESTION 2) inserting rows into fact table
--loading the new values into the fact_service_quality table for the new 
--service request values after 12th of February 2022.


--first recreeating the fact table (Not Part of assignment))
create or replace table fact_service_quality (
  agency_id number(8, 0) not null,
  location_zip varchar(50) NOT null,
  type_id number(8, 0) not null,
  yearweek number(8, 0) not null,
  count int not null default 0,
  avg float default null,
  sum int default null,
  min int default null,
  max int default null,
  primary key (agency_id, location_zip, type_id,yearweek)
  , constraint agency_dim foreign key (agency_id) references dim_agency (agency_id)
  , constraint location_dim foreign key (location_zip) references dim_location (location_zip)
  , constraint quest_type_dim foreign key (type_id) references dim_request_type (type_id)
  , constraint yearweek_dim foreign key (yearweek) references dim_yearweek (yearweek)
);

-- Populating the initial fact table. (Not Part of assignment)
insert into fact_service_quality (agency_id, location_zip, type_id, yearweek, count, avg,sum, min, max)
select dim_agency.agency_id, dim_location.location_zip, dim_request_type.type_id,
    sr_full.yearweek,
    count(*),
    avg(timestampdiff(hour, created_date, closed_date)),
    sum(timestampdiff(hour, created_date, closed_date)),
    min(timestampdiff(hour, created_date, closed_date)),
    max(timestampdiff(hour, created_date, closed_date))
from sr_full
inner join dim_agency dim_agency on lower(sr_full.Agency) = lower(dim_agency.agency_name)
inner join dim_location dim_location on sr_full.incident_zip_id = dim_location.location_zip
inner join dim_request_type dim_request_type on lower(sr_full.complaint_type_id) = lower(dim_request_type.type_id)
inner join dim_yearweek dim_yearweek on sr_full.yearweek = dim_yearweek.yearweek
group by dim_agency.agency_id, dim_location.location_zip, dim_request_type.type_id, sr_full.yearweek;
--1914246 rows

--Now populating fact table with new values as per question
insert into fact_service_quality(agency_id, location_zip, type_id, yearweek, count, sum, avg, min, max)
     with complaint_counts as (select "Complaint_Type" as "Complaint",Count("Unique_Key") as "COUNT" 
                               from nyc311.service_request_all 
                               group by "Complaint_Type"), -- alias table for future use
     --aliasing the full service request table as sr
     sr as (select "Agency","Incident_Zip",case --mimicking the processing on the complaint type dimesion
                                              when "Complaint_Type" like 'Misc%' then 'Miscellaneous'
                                              when "Complaint_Type" like '%Compliance%' then 'Compliance'
                                              when "Complaint_Type" like '%COVID%' then 'COVID'
                                              when lower("Complaint_Type") like '%Select%' then 'System Error'
                                              when "Complaint_Type" like '%../../%' then 'System Error'
                                              when "Complaint_Type" like '%....%' then 'System Error'
                                              when lower("Complaint_Type") like '%heat%' then 'Heating'
                                              when "Complaint_Type" like 'Paint' then 'Paint'
                                              when  "COUNT" <=2 then 'Other/Error'
                                              else "Complaint_Type"
                                            end as "Complaint_Type",to_date("Created_Date") as "Created_Date",to_date("Closed_Date") as "Closed_Date",to_number(date_part('year',"Created_Date") || date_part('week',"Created_Date"),8) as "Yearweek" --subquery to increase performance
      from NYC311.service_request_all t1 join (table complaint_counts) c on t1."Complaint_Type" = c."Complaint"
      where "Created_Date" > to_date('2022-02-12')-- filtering by only the values past specified created date (new values)
      and "Closed_Date" is not null
      and "Created_Date" <= "Closed_Date")--removing invalid date values
      
select da.agency_id, dl.location_zip, drt.type_id,sr."Yearweek",
    count(*),
    sum(timestampdiff(hour,"Created_Date","Closed_Date")),
    avg(timestampdiff(hour,"Created_Date","Closed_Date")),
    min(timestampdiff(hour,"Created_Date","Closed_Date")),
    max(timestampdiff(hour,"Created_Date","Closed_Date"))
from (table sr
     ) sr
inner join dim_agency da on lower(sr."Agency") = lower(da.agency_name)
inner join dim_location dl on sr."Incident_Zip" = dl.location_zip
inner join dim_request_type drt on lower(regexp_replace(sr."Complaint_Type", '[^[:alnum:]]+', '')) = lower(regexp_replace(drt.type_name, '[^[:alnum:]]+', '')) --join trying to match new all complaint types using regular expression and lower functions
inner join dim_yearweek dywk on  sr."Yearweek"= dywk.yearweek
group by da.agency_id, dl.location_zip, drt.type_id, sr."Yearweek";


--QUESTION 3) 
--a)Agency Breakdown. average service request processing time for each agency
--total hours divided by the total number of calls

select a."AGENCY_ID",round(sum("SUM")/sum("COUNT"),2) as "Avg_Service_Request_Processing_time (Hours)"
from fact_service_quality fact 
join dim_agency a on a.agency_id = fact.agency_id  --joining to dimension table
group by a."AGENCY_ID" --grouping by the agency
order by "Avg_Service_Request_Processing_time (Hours)" asc; 
-- ordering by Avg_Service_Request


--b) Borough Wise breakdown. 
-- joining to original map table and then to borough reference table
select "Borough",round(sum("SUM")/sum(fact."COUNT"),2) as "Avg_Service_Request_Processing_time (Hours)" --simple mathematical calculation to find average
from fact_service_quality fact 
    join map_incident_zip_nyc_borough b on fact."LOCATION_ZIP" = b."INCIDENT_ZIP" 
    join nyc311.zip_code_nyc_borough c on b."INCIDENT_ZIP" = c."Zip"
group by "Borough"
order by "Avg_Service_Request_Processing_time (Hours)" asc; --ordering from lowest to highest average processing time

--c)monthly breakdown of service requests

--firstly creating a date_dimesion table with multiple year facets
create or replace table date_dim as
select distinct date_trunc(month,"Created_Date") as "First_day_of_Month",to_number(date_part('year',"Created_Date") || date_part('week',"Created_Date")) as "YEARWEEK",MONTH("Created_Date") as "Month",YEAR("Created_Date") as "Year",monthname("Created_Date") as "Month_Name"
from nyc311.service_request_all
order by "First_day_of_Month" asc;
--ordering by first day of month

--use the newly created date dimension to group on both month and year
with t1 as (select "First_day_of_Month","Year",sum(COUNT) as "Total_Requests" --reference table built
            from fact_service_quality f
            join date_dim  dd on  dd."YEARWEEK" = f."YEARWEEK"
            group by "First_day_of_Month","Year"
            order by "Year","First_day_of_Month") --ordering by the month and not the month name 
select "Year",monthname("First_day_of_Month") as "Month_Name","Total_Requests" from (table t1) 

--QUESTION 4) with no_data warehouse Borough Breakdown. Average service request processing time for each Borough

with sr_selection as (select "Unique_Key",c."Borough",timestampdiff(hour,"Created_Date","Closed_Date") as "Hours"
                      from nyc311.service_request_all sr 
                      join map_incident_zip_nyc_borough b on sr."Incident_Zip" = b."INCIDENT_ZIP" 
                      join nyc311.zip_code_nyc_borough c on b."INCIDENT_ZIP" = c."Zip" --Joining to the brough reference table to obtain only orignial borough values
                      where "Created_Date" <"Closed_Date")--filtering negative hour values to remove illegitamate values
select "Borough",round(sum("Hours")/Count(*),2) as "Avg_Service_Request_Processing_time (Hours)"
from (table sr_selection)
group by "Borough"
order by "Avg_Service_Request_Processing_time (Hours)" asc;
